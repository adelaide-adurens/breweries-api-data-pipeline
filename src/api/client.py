"""Paginated client for Open Brewery DB API."""

import json
import logging
import math
import time
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

# HTTP status codes that trigger a retry (rate limit + server errors)
RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)


class APIError(Exception):
    """Raised when the API request fails."""

    def __init__(self, message: str, status_code: int | None = None, response_body: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


def _get(session: requests.Session, url: str, timeout: float) -> requests.Response:
    """Perform a single GET request. Raises RequestException on connection/HTTP errors."""
    resp = session.get(url, timeout=timeout)
    return resp


def _is_retryable_status(status_code: int) -> bool:
    """Return True if the status code is retryable (rate limit or server error)."""
    return status_code in RETRYABLE_STATUS_CODES


def _request_with_retry(
    session: requests.Session,
    url: str,
    *,
    max_retries: int,
    retry_delay: float,
    timeout: float,
) -> requests.Response:
    """
    GET with exponential backoff. Retries on retryable status codes (429, 5xx) and on RequestException.
    Raises APIError after exhausting retries or on non-retryable failure.
    """
    last_error: Exception | None = None
    delay = retry_delay
    for attempt in range(max_retries + 1):
        try:
            resp = _get(session, url, timeout)
            if resp.ok:
                return resp
            if _is_retryable_status(resp.status_code) and attempt < max_retries:
                logger.warning(
                    "Request to %s returned %s (attempt %d/%d), retrying in %.1fs",
                    url,
                    resp.status_code,
                    attempt + 1,
                    max_retries + 1,
                    delay,
                )
                time.sleep(delay)
                delay *= 2
                continue
            # Non-retryable or no retries left
            raise APIError(
                f"Request failed with status {resp.status_code}",
                status_code=resp.status_code,
                response_body=resp.text[:500],
            )
        except requests.RequestException as e:
            last_error = e
            if attempt < max_retries:
                logger.warning(
                    "Request to %s failed: %s (attempt %d/%d), retrying in %.1fs",
                    url,
                    e,
                    attempt + 1,
                    max_retries + 1,
                    delay,
                )
                time.sleep(delay)
                delay *= 2
            else:
                status_code = getattr(e.response, "status_code", None) if hasattr(e, "response") else None
                response_body = (
                    e.response.text[:500] if hasattr(e, "response") and e.response is not None else None
                )
                raise APIError(
                    f"Request failed after {max_retries + 1} attempts: {e}",
                    status_code=status_code,
                    response_body=response_body,
                ) from e
    raise APIError(f"Request failed: {last_error}") from last_error


def _parse_meta(resp: requests.Response) -> int:
    """Parse metadata response and return total count. Raises APIError on invalid content."""
    try:
        meta = resp.json()
    except ValueError as e:
        raise APIError(
            f"Invalid JSON in metadata response: {e}",
            response_body=resp.text[:500],
        ) from e
    total = meta.get("total")
    if total is None:
        raise APIError("Metadata missing 'total' field", response_body=resp.text[:500])
    return int(total)


def _parse_page(resp: requests.Response, page: int) -> list[dict[str, Any]]:
    """Parse a breweries list page response. Raises APIError on invalid content."""
    try:
        page_data = resp.json()
    except ValueError as e:
        raise APIError(
            f"Invalid JSON in page {page} response: {e}",
            response_body=resp.text[:500],
        ) from e
    if not isinstance(page_data, list):
        raise APIError(
            f"Expected list in page {page} response, got {type(page_data).__name__}",
            response_body=str(page_data)[:500],
        )
    return page_data


def fetch_breweries(
    base_url: str = "https://api.openbrewerydb.org/v1",
    per_page: int = 200,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    timeout: float = 30.0,
    return_total: bool = False,
) -> list[dict[str, Any]] | tuple[list[dict[str, Any]], int]:
    """
    Fetch all breweries from the Open Brewery DB API with pagination.

    Fetches metadata first to determine total count, then iterates through
    all pages. Uses a single Session and retries on 429 and 5xx (and connection errors).
    For large datasets prefer fetch_breweries_to_path() to avoid holding all data in memory.

    Args:
        base_url: API base URL.
        per_page: Number of breweries per page (max 200).
        max_retries: Maximum number of retries per request.
        retry_delay: Initial delay in seconds between retries (doubles each retry).
        timeout: Request timeout in seconds.

    Returns:
        List of brewery records as dicts. If return_total=True, returns (breweries, total).

    Raises:
        APIError: On HTTP errors, JSON parse errors, or after exhausting retries.
    """
    per_page = min(per_page, 200)
    breweries: list[dict[str, Any]] = []

    with requests.Session() as session:
        meta_url = f"{base_url}/breweries/meta"
        meta_resp = _request_with_retry(
            session, meta_url, max_retries=max_retries, retry_delay=retry_delay, timeout=timeout
        )
        total = _parse_meta(meta_resp)
        total_pages = math.ceil(total / per_page) if total else 0

        for page in range(1, total_pages + 1):
            list_url = f"{base_url}/breweries?page={page}&per_page={per_page}"
            list_resp = _request_with_retry(
                session, list_url, max_retries=max_retries, retry_delay=retry_delay, timeout=timeout
            )
            breweries.extend(_parse_page(list_resp, page))

    if return_total:
        return breweries, total
    return breweries


def fetch_breweries_to_path(
    output_path: str | Path,
    base_url: str = "https://api.openbrewerydb.org/v1",
    per_page: int = 200,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    timeout: float = 30.0,
) -> tuple[Path, int]:
    """
    Fetch all breweries and write to JSONL file page-by-page (streaming).

    Does not load the full dataset into memory: each page is fetched, parsed,
    and written to the file before the next page. Use this for large datasets
    and when passing data between processes (e.g. Airflow tasks).

    Returns:
        (path to written file, total count from metadata).
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.Session() as session:
        meta_url = f"{base_url}/breweries/meta"
        meta_resp = _request_with_retry(
            session, meta_url, max_retries=max_retries, retry_delay=retry_delay, timeout=timeout
        )
        total = _parse_meta(meta_resp)
        per_page = min(per_page, 200)
        total_pages = math.ceil(total / per_page) if total else 0

        logger.info("Fetching %d breweries in %d pages to %s", total, total_pages, output_path)

        with open(output_path, "w", encoding="utf-8") as f:
            for page in range(1, total_pages + 1):
                list_url = f"{base_url}/breweries?page={page}&per_page={per_page}"
                list_resp = _request_with_retry(
                    session,
                    list_url,
                    max_retries=max_retries,
                    retry_delay=retry_delay,
                    timeout=timeout,
                )
                page_data = _parse_page(list_resp, page)
                for record in page_data:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")

        logger.info("Wrote %d records to %s", total, output_path)
    return output_path, total
