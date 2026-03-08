"""Tests for the Bronze layer loader."""

import json
import tempfile
from pathlib import Path

import pytest

from src.bronze.loader import ValidationError, load_bronze, load_bronze_from_path

VALID_RECORD = {
    "id": "b1",
    "name": "Test Brewery",
    "brewery_type": "micro",
    "city": "Portland",
    "state_province": "Oregon",
    "country": "United States",
    "postal_code": "97201",
    "state": "Oregon",
}


def test_load_bronze_writes_valid_records(tmp_path):
    """Writes valid records to JSONL and returns path."""
    records = [VALID_RECORD, {**VALID_RECORD, "id": "b2"}]
    result = load_bronze(records, tmp_path, run_id="run-001")

    assert result.exists()
    assert result.name == "breweries.jsonl"
    lines = result.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["id"] == "b1"


def test_load_bronze_atomic_write_on_success(tmp_path):
    """Final file exists only after successful write (atomic)."""
    records = [VALID_RECORD]
    final = load_bronze(records, tmp_path, run_id="run-002")

    assert final.exists()
    tmp_files = list(Path(tmp_path).rglob(".tmp_*"))
    assert len(tmp_files) == 0


def test_load_bronze_validation_fails_on_missing_required(tmp_path):
    """Raises ValidationError when required fields are missing."""
    bad = {k: v for k, v in VALID_RECORD.items() if k != "postal_code"}
    bad["postal_code"] = None

    with pytest.raises(ValidationError) as exc_info:
        load_bronze([bad], tmp_path, run_id="run-003")

    assert "postal_code" in str(exc_info.value).lower() or "missing" in str(exc_info.value).lower()
    assert exc_info.value.invalid_count == 1


def test_load_bronze_validation_fails_on_empty_partition_key(tmp_path):
    """Raises ValidationError when partition key is empty."""
    bad = {**VALID_RECORD, "state_province": ""}

    with pytest.raises(ValidationError):
        load_bronze([bad], tmp_path, run_id="run-004")


def test_load_bronze_count_mismatch_raises(tmp_path):
    """Raises ValidationError when count deviates beyond tolerance."""
    records = [VALID_RECORD]

    with pytest.raises(ValidationError) as exc_info:
        load_bronze(records, tmp_path, run_id="run-005", expected_total=100, count_tolerance=0.05)

    assert "count" in str(exc_info.value).lower() or "mismatch" in str(exc_info.value).lower()


def test_load_bronze_count_within_tolerance_succeeds(tmp_path):
    """Succeeds when count is within tolerance."""
    records = [VALID_RECORD] * 96
    result = load_bronze(records, tmp_path, run_id="run-006", expected_total=100, count_tolerance=0.05)

    assert result.exists()
    assert len(result.read_text().strip().split("\n")) == 96


def test_load_bronze_from_path(tmp_path):
    """Reads from JSONL file and persists to Bronze."""
    source = tmp_path / "source.jsonl"
    source.write_text("\n".join(json.dumps(r) for r in [VALID_RECORD, {**VALID_RECORD, "id": "b2"}]))

    result = load_bronze_from_path(source, tmp_path / "bronze", run_id="run-007", expected_total=2)

    assert result.exists()
    assert len(result.read_text().strip().split("\n")) == 2
