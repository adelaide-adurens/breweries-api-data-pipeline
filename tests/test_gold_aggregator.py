"""Tests for the Gold layer aggregator."""

from pathlib import Path

import pandas as pd
import pytest
from deltalake import write_deltalake

from src.gold.aggregator import aggregate_to_gold


def _create_silver_fixture(tmp_path, data: list[dict]) -> Path:
    """Create a minimal Silver Delta table for testing."""
    df = pd.DataFrame(data)
    silver_path = tmp_path / "silver" / "breweries"
    silver_path.mkdir(parents=True)
    write_deltalake(str(silver_path), df, mode="overwrite")
    return silver_path


def test_aggregate_to_gold_counts_correctly(tmp_path):
    """Aggregates breweries by type and location with correct counts."""
    silver_data = [
        {"id": "b1", "brewery_type": "micro", "country": "US", "state_province": "Oregon"},
        {"id": "b2", "brewery_type": "micro", "country": "US", "state_province": "Oregon"},
        {"id": "b3", "brewery_type": "brewpub", "country": "US", "state_province": "Oregon"},
    ]
    silver_path = _create_silver_fixture(tmp_path, silver_data)
    gold_path = tmp_path / "gold" / "breweries_by_type_location"

    result = aggregate_to_gold(silver_path, gold_path)

    parquet_file = result / "breweries_by_type_location.parquet"
    assert parquet_file.exists()
    df = pd.read_parquet(parquet_file)
    assert len(df) == 2
    micro_row = df[(df["brewery_type"] == "micro") & (df["state_province"] == "Oregon")]
    assert micro_row["brewery_count"].iloc[0] == 2
    brewpub_row = df[(df["brewery_type"] == "brewpub") & (df["state_province"] == "Oregon")]
    assert brewpub_row["brewery_count"].iloc[0] == 1


def test_aggregate_to_gold_schema(tmp_path):
    """Output has expected schema: brewery_type, country, state_province, brewery_count, aggregated_at."""
    silver_data = [
        {"id": "b1", "brewery_type": "micro", "country": "US", "state_province": "CA"},
    ]
    silver_path = _create_silver_fixture(tmp_path, silver_data)
    gold_path = tmp_path / "gold"

    result = aggregate_to_gold(silver_path, gold_path)

    df = pd.read_parquet(result / "breweries_by_type_location.parquet")
    assert set(df.columns) == {"brewery_type", "country", "state_province", "brewery_count", "aggregated_at"}


def test_aggregate_to_gold_empty_silver_raises(tmp_path):
    """Raises when Silver table is empty."""
    from unittest.mock import patch

    silver_path = tmp_path / "silver" / "breweries"
    silver_path.mkdir(parents=True)
    write_deltalake(str(silver_path), pd.DataFrame({"id": ["x"], "brewery_type": ["micro"], "country": ["US"], "state_province": ["CA"]}), mode="overwrite")

    with patch("src.gold.aggregator.DeltaTable") as mock_dt:
        mock_dt.return_value.to_pandas.return_value = pd.DataFrame()

        with pytest.raises(ValueError) as exc_info:
            aggregate_to_gold(silver_path, tmp_path / "gold")

        assert "empty" in str(exc_info.value).lower()
