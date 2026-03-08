# Breweries API Data Pipeline

A Dockerized Airflow data pipeline that fetches breweries from the [Open Brewery DB API](https://www.openbrewerydb.org/), persists raw data in Bronze, transforms to Delta Lake in Silver (partitioned by location), and aggregates to Gold—following the medallion (lakehouse) architecture.

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.10+ (for local development and tests)

### Run with Docker

1. **Prepare environment**

   ```bash
   mkdir -p dags logs plugins data/bronze data/silver data/gold data/staging
   echo "AIRFLOW_UID=$(id -u 2>/dev/null || echo 50000)" > .env
   cp .env.example .env
   # Edit .env with your SMTP settings for email alerts (see Email Setup below)
   ```

2. **Initialize Airflow**

   ```bash
   docker compose up airflow-init
   ```

3. **Start services**

   ```bash
   docker compose up -d
   ```

4. **Access Airflow UI**

   Open http://localhost:8080 (login: `admin` / `admin`). Unpause the `breweries_pipeline` DAG and trigger a run if needed.

5. **Configure Variables** (optional)

   In Airflow UI: Admin → Variables → Add:
   - `data_lake_root`: `/opt/airflow/data` (default) — root for all layer paths
   - `staging_base_path`, `bronze_base_path`, `silver_breweries_path`, `gold_breweries_path`: override per-layer paths (defaults derived from `data_lake_root`)
   - `alert_email_to`: your email for failure alerts (comma-separated for multiple)

### Run Tests Locally

```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -e ".[dev]"
pytest tests/ -v
```

## Architecture

- **Bronze**: Raw JSONL from API, one file per run, atomic writes
- **Silver**: Delta Lake, partitioned by `country` and `state_province`
- **Gold**: Parquet with breweries count per type and location

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for details.

## Data Validation

After a pipeline run, you can verify that each layer meets requirements using the validation notebook:

- **[notebooks/data_validation_medallion.ipynb](notebooks/data_validation_medallion.ipynb)** — Checks Bronze (required fields, partition keys), Silver (schema, deduplication, partitions), and Gold (schema, count consistency with Silver). Run all cells to produce a documented validation report.

## Email Setup

To receive alerts on DAG/task failures, configure SMTP. See [docs/EMAIL_SETUP.md](docs/EMAIL_SETUP.md) for step-by-step instructions for Gmail, SendGrid, Outlook, and other providers.

## Documentation

| Document | Description |
|---------|-------------|
| [docs/PLAN.md](docs/PLAN.md) | Implementation plan |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Design and data flow |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Trade-offs and rationale |
| [docs/EMAIL_SETUP.md](docs/EMAIL_SETUP.md) | Email/SMTP configuration |
| [docs/PRACTICES_REVIEW.md](docs/PRACTICES_REVIEW.md) | Python, SE, and Airflow practices review |
| [docs/AI_INTERACTIONS.md](docs/AI_INTERACTIONS.md) | Log of AI-assisted development interactions |

## Next steps (improvements)

Possible directions to extend or harden this solution:

- **Cloud + IaC** — Move to a cloud provider (e.g. AWS, GCP, Azure) and define infrastructure as code (e.g. **Terraform**) for the data lake (S3/GCS/ADLS), compute, and Airflow (MWAA, Cloud Composer, or self-managed on EKS/GKE).
- **CI/CD** — Add a **CI/CD pipeline** that runs tests (e.g. `pytest`), linting/formatting, and deploys the pipeline to a cloud environment.
- **Real Delta merge at scale** — For large Silver tables, replace the current in-memory merge + full overwrite with a true **Delta MERGE** (e.g. via Spark SQL or Delta Standalone) so only changed rows are written; see [docs/DECISIONS.md](docs/DECISIONS.md).
- **Observability** — Add metrics, structured logging, and alerting (e.g. task/DAG SLAs, data freshness). Optionally integrate **OpenLineage** (or similar) with Airflow for data lineage.
- **Data quality** — Add automated checks (e.g. **Great Expectations**) on Bronze/Silver/Gold (nulls, row counts, schema) and fail the DAG or send alerts when checks fail.
- **Secrets and config** — Use a secrets manager (e.g. AWS Secrets Manager, HashiCorp Vault) and environment-specific config instead of plain `.env` for production.
- **Staging vs production** — Separate staging and production environments (e.g. different buckets, Airflow deployments) and run the validation notebook or data-quality jobs in CI against staging.
