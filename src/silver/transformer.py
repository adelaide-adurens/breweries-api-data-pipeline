"""Silver layer transformer: Bronze JSONL to Delta Lake with partitioning."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

logger = logging.getLogger(__name__)

# Read Bronze in chunks to bound memory (no full-file read into a single string/list).
CHUNK_SIZE = 10_000

COLUMNS_TO_KEEP = [
    "id",
    "name",
    "brewery_type",
    "address_1",
    "address_2",
    "address_3",
    "city",
    "state_province",
    "postal_code",
    "country",
    "longitude",
    "latitude",
    "phone",
    "website_url",
]
# Lineage and dedup: kept in Silver for lakehouse traceability and "keep last by ingested_at".
METADATA_COLUMNS = ["ingested_at", "source_file"]


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    """Normalize a brewery record: keep allowed columns, coerce types."""
    out: dict[str, Any] = {}
    for col in COLUMNS_TO_KEEP:
        val = record.get(col)
        if col in ("longitude", "latitude") and val is not None:
            try:
                out[col] = float(val)
            except (TypeError, ValueError):
                out[col] = None
        else:
            out[col] = val
    return out


def _iter_bronze_records(
    paths: list[Path],
    run_timestamp: datetime,
) -> Iterator[dict[str, Any]]:
    """
    Yield normalized records from Bronze JSONL files, line by line.
    Does not load full file into memory. Each record includes ingested_at and source_file.
    """
    for p in paths:
        source_label = p.name
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                normalized = _normalize_record(record)
                normalized["ingested_at"] = run_timestamp
                normalized["source_file"] = source_label
                yield normalized


def _read_bronze_into_dataframe(
    paths: list[Path],
    run_timestamp: datetime,
) -> pd.DataFrame:
    """Read Bronze JSONL in chunks, build a single DataFrame. Bounds memory by CHUNK_SIZE per batch."""
    chunk: list[dict[str, Any]] = []
    chunks_dfs: list[pd.DataFrame] = []
    for rec in _iter_bronze_records(paths, run_timestamp):
        chunk.append(rec)
        if len(chunk) >= CHUNK_SIZE:
            chunks_dfs.append(pd.DataFrame(chunk))
            chunk = []
    if chunk:
        chunks_dfs.append(pd.DataFrame(chunk))
    if not chunks_dfs:
        return pd.DataFrame()
    return pd.concat(chunks_dfs, ignore_index=True)


def transform_to_silver(
    bronze_path: str | Path,
    silver_path: str | Path,
    partition_by: tuple[str, ...] = ("country", "state_province"),
    run_timestamp: datetime | None = None,
    merge_with_existing: bool = True,
) -> Path:
    """
    Read Bronze JSONL, transform and write to Silver Delta table.

    Transformations:
    - Keep only relevant columns (drop deprecated state, street)
    - Coerce longitude/latitude to float
    - Add ingested_at (run timestamp) and source_file (Bronze file name) for lineage and dedup
    - Deduplicate by id (keep last by ingested_at)
    - Partition by country and state_province

    When merge_with_existing is True (default), reads existing Silver if present and merges:
    new Bronze rows are combined with existing Silver, then deduped by id (keep latest ingested_at).
    This avoids full overwrite and only updates/changes from the new Bronze run.

    Args:
        bronze_path: Path to Bronze JSONL file or directory of JSONL files.
        silver_path: Path to Silver Delta table root.
        partition_by: Partition columns.
        run_timestamp: Timestamp for this run (ingested_at). Defaults to utcnow().
        merge_with_existing: If True, merge new Bronze with existing Silver (upsert by id).

    Returns:
        Path to the Silver Delta table.
    """
    bronze_path = Path(bronze_path)
    silver_path = Path(silver_path)

    if run_timestamp is None:
        run_timestamp = datetime.now(timezone.utc)

    if bronze_path.is_file():
        paths = [bronze_path]
    else:
        paths = list(bronze_path.rglob("*.jsonl"))

    if not paths:
        raise FileNotFoundError(f"No JSONL files found under {bronze_path}")

    logger.info("Reading Bronze in chunks (chunk_size=%d) from %d file(s)", CHUNK_SIZE, len(paths))
    df_new = _read_bronze_into_dataframe(paths, run_timestamp)

    if df_new.empty:
        raise ValueError(f"No records found in {bronze_path}")

    if merge_with_existing and silver_path.exists() and (silver_path / "_delta_log").exists():
        try:
            dt = DeltaTable(str(silver_path))
            df_existing = dt.to_pandas()
            if not df_existing.empty:
                for col in METADATA_COLUMNS:
                    if col not in df_existing.columns:
                        df_existing[col] = pd.NaT if col == "ingested_at" else ""
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                df = df_combined.sort_values("ingested_at", na_position="first").drop_duplicates(
                    subset=["id"], keep="last"
                )
                logger.info("Merged with existing Silver: %d existing + %d new -> %d after dedup",
                    len(df_existing), len(df_new), len(df))
            else:
                df = df_new.sort_values("ingested_at").drop_duplicates(subset=["id"], keep="last")
        except Exception as e:
            logger.warning("Could not read existing Silver for merge, overwriting: %s", e)
            df = df_new.sort_values("ingested_at").drop_duplicates(subset=["id"], keep="last")
    else:
        df = df_new.sort_values("ingested_at").drop_duplicates(subset=["id"], keep="last")

    for col in partition_by:
        if col not in df.columns:
            raise ValueError(f"Partition column '{col}' not in data")
        df[col] = df[col].fillna("").astype(str)

    for col in df.columns:
        if col == "ingested_at":
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                continue
            df[col] = pd.to_datetime(df[col], utc=True)
        elif df[col].dtype == "object" or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].fillna("").astype(str)
        elif pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].astype("float64")

    silver_path.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    write_deltalake(
        str(silver_path),
        table,
        partition_by=list(partition_by),
        mode="overwrite",
        schema_mode="overwrite",
    )

    logger.info("Silver transform complete: %d records in %s", len(df), silver_path)
    return silver_path
