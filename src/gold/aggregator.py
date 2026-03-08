"""Gold layer aggregator: breweries per type and location."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable

logger = logging.getLogger(__name__)


def aggregate_to_gold(
    silver_path: str | Path,
    gold_path: str | Path,
    aggregation_keys: tuple[str, ...] = ("brewery_type", "country", "state_province"),
    aggregated_at: datetime | None = None,
) -> Path:
    """
    Aggregate Silver Delta table to Gold Parquet: count of breweries per type and location.

    Reads the Silver Delta table, groups by aggregation_keys, and writes the aggregated
    result as Parquet with overwrite per run. Includes aggregated_at timestamp for lineage.

    Args:
        silver_path: Path to Silver Delta table root.
        gold_path: Path to Gold output directory (Parquet files).
        aggregation_keys: Columns to group by for aggregation.
        aggregated_at: Timestamp for this Gold snapshot (default: utcnow()).

    Returns:
        Path to the Gold output directory.
    """
    silver_path = Path(silver_path)
    gold_path = Path(gold_path)

    if aggregated_at is None:
        aggregated_at = datetime.now(timezone.utc)

    dt = DeltaTable(str(silver_path))
    df = dt.to_pandas()

    if df.empty:
        raise ValueError(f"Silver table at {silver_path} is empty")

    for col in aggregation_keys:
        if col not in df.columns:
            raise ValueError(f"Aggregation column '{col}' not in Silver data")

    agg = (
        df.groupby(list(aggregation_keys), dropna=False)
        .size()
        .reset_index(name="brewery_count")
    )
    agg["aggregated_at"] = aggregated_at

    gold_path.mkdir(parents=True, exist_ok=True)
    out_file = gold_path / "breweries_by_type_location.parquet"
    agg.to_parquet(out_file, index=False)

    logger.info("Gold aggregation complete: %d rows in %s", len(agg), gold_path)
    return gold_path
