"""Bronze layer loader: validate and persist raw API data."""

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

REQUIRED_FIELDS = frozenset(
    {"id", "name", "brewery_type", "city", "state_province", "country", "postal_code", "state"}
)
PARTITION_KEYS = frozenset({"country", "state_province"})

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when data validation fails."""

    def __init__(self, message: str, invalid_count: int = 0, sample_invalid: list[dict] | None = None):
        super().__init__(message)
        self.invalid_count = invalid_count
        self.sample_invalid = sample_invalid or []


def _validate_record(record: dict[str, Any]) -> list[str]:
    """Validate a single brewery record. Returns list of validation errors."""
    errors: list[str] = []
    if not isinstance(record, dict):
        return [f"Record is not a dict: {type(record).__name__}"]

    for field in REQUIRED_FIELDS:
        val = record.get(field)
        if val is None or (isinstance(val, str) and not val.strip()):
            errors.append(f"Missing or empty required field: {field}")

    for key in PARTITION_KEYS:
        if key in record and (record[key] is None or (isinstance(record[key], str) and not str(record[key]).strip())):
            errors.append(f"Partition key '{key}' is null or empty")

    return errors


def load_bronze(
    breweries: list[dict[str, Any]],
    base_path: str | Path,
    run_id: str,
    expected_total: int | None = None,
    count_tolerance: float = 0.05,
) -> Path:
    """
    Validate brewery records and persist to Bronze layer as JSONL.

    Uses atomic write: writes to a temp file first, then renames to final path
    only on success. If validation fails or an error occurs mid-write, no file
    is persisted, guaranteeing integrity.

    Args:
        breweries: List of brewery records from the API.
        base_path: Base directory for Bronze layer (e.g. data/bronze).
        run_id: Unique identifier for this run.
        expected_total: Expected total count from metadata (for validation).
        count_tolerance: Allowed relative deviation in count (default 5%).

    Returns:
        Path to the written file.

    Raises:
        ValidationError: When validation fails (critical fields, count mismatch).
    """
    base_path = Path(base_path)
    run_dir = base_path / "breweries" / f"run_id={run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    final_path = run_dir / "breweries.jsonl"

    valid_records: list[dict[str, Any]] = []
    invalid_records: list[tuple[dict[str, Any], list[str]]] = []

    for record in breweries:
        errs = _validate_record(record)
        if errs:
            invalid_records.append((record, errs))
        else:
            valid_records.append(record)

    if invalid_records:
        sample = [r for r, _ in invalid_records[:5]]
        raise ValidationError(
            f"Validation failed: {len(invalid_records)} invalid records. "
            f"Required fields: {sorted(REQUIRED_FIELDS)}. "
            f"Sample errors: {invalid_records[0][1]}",
            invalid_count=len(invalid_records),
            sample_invalid=sample,
        )

    if expected_total is not None:
        diff = abs(len(valid_records) - expected_total)
        if expected_total > 0 and diff / expected_total > count_tolerance:
            raise ValidationError(
                f"Record count mismatch: got {len(valid_records)}, expected {expected_total} "
                f"(tolerance {count_tolerance:.0%})"
            )

    fd, tmp_path = tempfile.mkstemp(suffix=".jsonl", dir=run_dir, prefix=".tmp_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for record in valid_records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        os.replace(tmp_path, final_path)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        raise

    logger.info("Bronze load complete: %d records written to %s", len(valid_records), final_path)
    return final_path


def load_bronze_from_path(
    source_path: str | Path,
    base_path: str | Path,
    run_id: str,
    expected_total: int | None = None,
    count_tolerance: float = 0.05,
) -> Path:
    """
    Read brewery records from a JSONL file, validate and persist to Bronze row-by-row.

    Streams the source file: reads line by line, validates each record, and writes
    valid rows to a temp file immediately. Only a small sample of invalid records
    is kept in memory, so this is safe for large files.

    Args:
        source_path: Path to source JSONL file.
        base_path: Base directory for Bronze layer.
        run_id: Unique identifier for this run.
        expected_total: Expected total count for validation.
        count_tolerance: Allowed relative deviation in count.

    Returns:
        Path to the written Bronze file.
    """
    source_path = Path(source_path)
    base_path = Path(base_path)
    run_dir = base_path / "breweries" / f"run_id={run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    final_path = run_dir / "breweries.jsonl"

    fd, tmp_path = tempfile.mkstemp(suffix=".jsonl", dir=run_dir, prefix=".tmp_")
    written_count = 0
    invalid_count = 0
    sample_invalid: list[dict[str, Any]] = []
    first_invalid_errors: list[str] | None = None

    def _cleanup() -> None:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    try:
        with open(source_path, "r", encoding="utf-8") as src, os.fdopen(fd, "w", encoding="utf-8") as dst:
            for line in src:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except ValueError as e:
                    invalid_count += 1
                    if len(sample_invalid) < 5:
                        sample_invalid.append({"_parse_error": str(e), "_line_preview": line[:100]})
                    if first_invalid_errors is None:
                        first_invalid_errors = [f"Invalid JSON: {e}"]
                    continue
                if not isinstance(record, dict):
                    invalid_count += 1
                    if len(sample_invalid) < 5:
                        sample_invalid.append(record)
                    if first_invalid_errors is None:
                        first_invalid_errors = [f"Record is not a dict: {type(record).__name__}"]
                    continue
                errs = _validate_record(record)
                if errs:
                    invalid_count += 1
                    if len(sample_invalid) < 5:
                        sample_invalid.append(record)
                    if first_invalid_errors is None:
                        first_invalid_errors = errs
                    continue
                dst.write(json.dumps(record, ensure_ascii=False) + "\n")
                written_count += 1
    except Exception:
        _cleanup()
        raise

    if invalid_count > 0:
        _cleanup()
        raise ValidationError(
            f"Validation failed: {invalid_count} invalid records. "
            f"Required fields: {sorted(REQUIRED_FIELDS)}. "
            f"Sample errors: {first_invalid_errors or []}",
            invalid_count=invalid_count,
            sample_invalid=sample_invalid,
        )

    if expected_total is not None:
        if expected_total > 0 and abs(written_count - expected_total) / expected_total > count_tolerance:
            _cleanup()
            raise ValidationError(
                f"Record count mismatch: got {written_count}, expected {expected_total} "
                f"(tolerance {count_tolerance:.0%})"
            )

    os.replace(tmp_path, final_path)
    logger.info("Bronze load complete: %d records written to %s", written_count, final_path)
    return final_path
