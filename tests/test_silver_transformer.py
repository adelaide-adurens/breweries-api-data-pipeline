"""Tests for the Silver layer transformer."""

import json
from pathlib import Path

import pandas as pd
import pytest
from deltalake import DeltaTable

from src.silver.transformer import transform_to_silver

SAMPLE_RECORDS = [
    {
        "id": "b1",
        "name": "Brewery One",
        "brewery_type": "micro",
        "city": "Portland",
        "state_province": "Oregon",
        "country": "United States",
        "postal_code": "97201",
        "address_1": "123 Main St",
        "longitude": -122.6,
        "latitude": 45.5,
    },
    {
        "id": "b2",
        "name": "Brewery Two",
        "brewery_type": "brewpub",
        "city": "Seattle",
        "state_province": "Washington",
        "country": "United States",
        "postal_code": "98101",
        "address_1": None,
        "longitude": None,
        "latitude": None,
    },
    {
        "id": "b1",
        "name": "Brewery One Updated",
        "brewery_type": "micro",
        "city": "Portland",
        "state_province": "Oregon",
        "country": "United States",
        "postal_code": "97201",
        "address_1": "123 Main St",
        "longitude": -122.6,
        "latitude": 45.5,
    },
]


def test_transform_to_silver_partitions(tmp_path):
    """Writes Delta table partitioned by country and state_province."""
    bronze_dir = tmp_path / "bronze" / "breweries" / "run_id=test"
    bronze_dir.mkdir(parents=True)
    bronze_file = bronze_dir / "breweries.jsonl"
    bronze_file.write_text("\n".join(json.dumps(r) for r in SAMPLE_RECORDS))

    silver_path = tmp_path / "silver" / "breweries"
    result = transform_to_silver(bronze_file, silver_path)

    assert result.exists()
    dt = DeltaTable(str(result))
    df = dt.to_pandas()
    assert len(df) == 2
    assert set(df["id"]) == {"b1", "b2"}
    assert df[df["id"] == "b1"]["name"].iloc[0] == "Brewery One Updated"
    assert "country" in df.columns and "state_province" in df.columns


def test_transform_to_silver_deduplicates_by_id(tmp_path):
    """Keeps last occurrence when duplicate ids exist."""
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    (bronze_dir / "breweries.jsonl").write_text(
        "\n".join(json.dumps(r) for r in SAMPLE_RECORDS)
    )

    silver_path = tmp_path / "silver" / "breweries"
    transform_to_silver(bronze_dir, silver_path)

    dt = DeltaTable(str(silver_path))
    df = dt.to_pandas()
    assert df[df["id"] == "b1"]["name"].iloc[0] == "Brewery One Updated"


def test_transform_to_silver_coerces_lat_long(tmp_path):
    """Coerces latitude/longitude to float."""
    records = [
        {
            **SAMPLE_RECORDS[0],
            "longitude": "-122.6",
            "latitude": "45.5",
        }
    ]
    bronze_file = tmp_path / "breweries.jsonl"
    bronze_file.write_text(json.dumps(records[0]))

    silver_path = tmp_path / "silver" / "breweries"
    transform_to_silver(bronze_file, silver_path)

    dt = DeltaTable(str(silver_path))
    df = dt.to_pandas()
    assert df["longitude"].iloc[0] == pytest.approx(-122.6)
    assert df["latitude"].iloc[0] == pytest.approx(45.5)


def test_transform_to_silver_empty_dir_raises(tmp_path):
    """Raises when no JSONL files found."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    with pytest.raises(FileNotFoundError):
        transform_to_silver(empty_dir, tmp_path / "silver" / "breweries")


def test_transform_to_silver_adds_ingested_at_and_source_file(tmp_path):
    """Silver table includes ingested_at and source_file for lineage and dedup."""
    from datetime import datetime, timezone

    bronze_file = tmp_path / "breweries.jsonl"
    bronze_file.write_text(json.dumps(SAMPLE_RECORDS[0]))
    silver_path = tmp_path / "silver" / "breweries"
    run_ts = datetime(2026, 1, 7, 12, 0, 0, tzinfo=timezone.utc)

    transform_to_silver(bronze_file, silver_path, run_timestamp=run_ts)

    dt = DeltaTable(str(silver_path))
    df = dt.to_pandas()
    assert "ingested_at" in df.columns and "source_file" in df.columns
    assert df["source_file"].iloc[0] == "breweries.jsonl"
    assert pd.Timestamp(df["ingested_at"].iloc[0]).tz_convert(timezone.utc) == run_ts


def test_transform_to_silver_merge_with_existing(tmp_path):
    """When merge_with_existing=True, new Bronze is merged with existing Silver; id dedup keeps latest ingested_at."""
    from datetime import datetime, timezone

    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    silver_path = tmp_path / "silver" / "breweries"

    # First run: one record
    (bronze_dir / "run1.jsonl").write_text(json.dumps({**SAMPLE_RECORDS[0], "id": "b1", "name": "First"}))
    transform_to_silver(bronze_dir, silver_path, run_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc))

    df1 = DeltaTable(str(silver_path)).to_pandas()
    assert len(df1) == 1
    assert df1["name"].iloc[0] == "First"

    # Second run: same id, updated name (merge)
    (bronze_dir / "run2.jsonl").write_text(json.dumps({**SAMPLE_RECORDS[0], "id": "b1", "name": "Second"}))
    transform_to_silver(bronze_dir, silver_path, run_timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc), merge_with_existing=True)

    df2 = DeltaTable(str(silver_path)).to_pandas()
    assert len(df2) == 1
    assert df2["name"].iloc[0] == "Second"
