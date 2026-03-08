"""
Breweries data pipeline DAG.

Fetches data from Open Brewery DB API, persists to Bronze, transforms to Silver (Delta),
and aggregates to Gold. Schedule: daily (breweries data changes infrequently).
"""

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

logger = logging.getLogger(__name__)

DEFAULT_DATA_LAKE_ROOT = "/opt/airflow/data"


def _get_variable(name: str, default: str) -> str:
    """Load an Airflow Variable with fallback and warning log on failure."""
    try:
        return Variable.get(name, default_var=default)
    except Exception as e:
        logger.warning(
            "Could not load Variable %r, using default %r: %s",
            name,
            default,
            e,
            exc_info=True,
        )
        return default


def _get_data_lake_root() -> str:
    return _get_variable("data_lake_root", DEFAULT_DATA_LAKE_ROOT)


DATA_LAKE_ROOT = _get_data_lake_root()

# Layer path variables (override in Airflow UI to change locations)
STAGING_BASE = _get_variable("staging_base_path", f"{DATA_LAKE_ROOT}/staging")
BRONZE_BASE = _get_variable("bronze_base_path", f"{DATA_LAKE_ROOT}/bronze")
SILVER_BREWERIES_PATH = _get_variable("silver_breweries_path", f"{DATA_LAKE_ROOT}/silver/breweries")
GOLD_BREWERIES_PATH = _get_variable("gold_breweries_path", f"{DATA_LAKE_ROOT}/gold/breweries_by_type_location")


def _get_default_args() -> dict:
    from datetime import timedelta

    email_to = _get_variable("alert_email_to", "")
    return {
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": bool(email_to),
        "email": [e.strip() for e in email_to.split(",") if e.strip()] if email_to else [],
    }


def _run_id_from_context(context: dict) -> str:
    """Build a filesystem-safe run_id from logical date, with ':' replaced by '-'."""
    logical_date = context.get("logical_date") or context.get("data_interval_start") or context.get("execution_date")
    if logical_date is not None:
        return str(logical_date).replace(":", "-")
    return context.get("run_id", "manual").replace(":", "-")


@dag(
    dag_id="breweries_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["breweries", "medallion"],
    default_args=_get_default_args(),
)
def breweries_pipeline():
    """Orchestrate breweries ETL: API -> Bronze -> Silver -> Gold."""

    @task
    def fetch_breweries_task():
        from pathlib import Path

        from airflow.operators.python import get_current_context

        from src.api.client import fetch_breweries_to_path

        context = get_current_context()
        run_id = _run_id_from_context(context)
        staging_dir = Path(STAGING_BASE) / f"run_id={run_id}"
        staging_dir.mkdir(parents=True, exist_ok=True)
        staging_path = staging_dir / "breweries.jsonl"
        logger.info("Fetching breweries to %s (run_id=%s)", staging_path, run_id)
        path, total = fetch_breweries_to_path(str(staging_path))
        logger.info("Fetched %d breweries to %s", total, path)
        return {"path": str(path), "total": total}

    @task
    def load_bronze_task(fetch_result: dict):
        from airflow.operators.python import get_current_context

        from src.bronze.loader import load_bronze_from_path

        context = get_current_context()
        run_id = _run_id_from_context(context)
        logger.info(
            "Loading Bronze from %s to base %s (run_id=%s, expected_total=%s)",
            fetch_result["path"],
            BRONZE_BASE,
            run_id,
            fetch_result["total"],
        )
        path = load_bronze_from_path(
            source_path=fetch_result["path"],
            base_path=BRONZE_BASE,
            run_id=run_id,
            expected_total=fetch_result["total"],
        )
        logger.info("Bronze load complete: %s", path)
        return str(path)

    @task
    def transform_silver_task(bronze_path: str):
        from datetime import timezone

        from airflow.operators.python import get_current_context

        from src.silver.transformer import transform_to_silver

        context = get_current_context()
        logical_date = context.get("logical_date") or context.get("data_interval_start") or context.get("execution_date")
        run_ts = None
        if logical_date is not None:
            run_ts = logical_date if logical_date.tzinfo else logical_date.replace(tzinfo=timezone.utc)

        logger.info("Transforming to Silver: %s -> %s", bronze_path, SILVER_BREWERIES_PATH)
        silver_path = transform_to_silver(
            bronze_path,
            silver_path=SILVER_BREWERIES_PATH,
            run_timestamp=run_ts,
            merge_with_existing=True,
        )
        logger.info("Silver transform complete: %s", silver_path)
        return str(silver_path)

    @task
    def aggregate_gold_task(silver_path: str):
        from datetime import timezone

        from airflow.operators.python import get_current_context

        from src.gold.aggregator import aggregate_to_gold

        context = get_current_context()
        logical_date = context.get("logical_date") or context.get("data_interval_start") or context.get("execution_date")
        aggregated_ts = None
        if logical_date is not None:
            aggregated_ts = logical_date if logical_date.tzinfo else logical_date.replace(tzinfo=timezone.utc)

        logger.info("Aggregating to Gold: %s -> %s", silver_path, GOLD_BREWERIES_PATH)
        gold_path = aggregate_to_gold(
            silver_path,
            gold_path=GOLD_BREWERIES_PATH,
            aggregated_at=aggregated_ts,
        )
        logger.info("Gold aggregation complete: %s", gold_path)
        return str(gold_path)

    fetch_result = fetch_breweries_task()
    bronze_path = load_bronze_task(fetch_result)
    silver_path = transform_silver_task(bronze_path)
    aggregate_gold_task(silver_path)


breweries_pipeline()
