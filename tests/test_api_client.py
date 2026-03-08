"""Tests for the Open Brewery DB API client."""

import json

import pytest
import responses

from src.api.client import APIError, fetch_breweries, fetch_breweries_to_path


@responses.activate
def test_fetch_breweries_pagination():
    """Fetches all pages and returns combined results."""
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries/meta",
        json={"total": 250, "page": 1, "per_page": 50},
    )
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=200",
        json=[{"id": f"b{i}", "name": f"Brewery {i}"} for i in range(200)],
    )
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries?page=2&per_page=200",
        json=[{"id": "b200", "name": "Brewery 200"}],
    )

    result = fetch_breweries(per_page=200)

    assert len(result) == 201
    assert result[0]["id"] == "b0"
    assert result[-1]["id"] == "b200"


@responses.activate
def test_fetch_breweries_respects_per_page_max():
    """Uses max 200 per_page even if higher value passed."""
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries/meta",
        json={"total": 10},
    )
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=200",
        json=[{"id": str(i)} for i in range(10)],
    )

    fetch_breweries(per_page=500)

    assert "per_page=200" in responses.calls[1].request.url


@responses.activate
def test_fetch_breweries_meta_missing_total_raises():
    """Raises APIError when metadata lacks total."""
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries/meta",
        json={"page": 1},
    )

    with pytest.raises(APIError) as exc_info:
        fetch_breweries()

    assert "total" in str(exc_info.value).lower()


@responses.activate
def test_fetch_breweries_invalid_json_raises():
    """Raises APIError on invalid JSON response."""
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries/meta",
        body="not json",
    )

    with pytest.raises(APIError) as exc_info:
        fetch_breweries()

    assert "JSON" in str(exc_info.value)


@responses.activate
def test_fetch_breweries_http_error_raises():
    """Raises APIError on HTTP 500."""
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries/meta",
        json={"total": 10},
    )
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=200",
        json={"error": "internal"},
        status=500,
    )

    with pytest.raises(APIError):
        fetch_breweries(max_retries=1)


@responses.activate
def test_fetch_breweries_retries_on_429():
    """Retries on rate limit (429) and eventually succeeds."""
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries/meta",
        json={"total": 5},
    )
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=200",
        status=429,
    )
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=200",
        json=[{"id": str(i)} for i in range(5)],
    )

    result = fetch_breweries(max_retries=2, retry_delay=0.01)

    assert len(result) == 5
    assert len([c for c in responses.calls if "breweries?page=1" in c.request.url]) == 2


@responses.activate
def test_fetch_breweries_to_path_streams_to_file(tmp_path):
    """Writes pages to file incrementally; does not require full dataset in memory."""
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries/meta",
        json={"total": 3},
    )
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=200",
        json=[{"id": "a", "name": "A"}, {"id": "b", "name": "B"}, {"id": "c", "name": "C"}],
    )
    out = tmp_path / "breweries.jsonl"

    path, total = fetch_breweries_to_path(out)

    assert path == out
    assert total == 3
    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 3
    assert json.loads(lines[0])["id"] == "a"
    assert json.loads(lines[2])["id"] == "c"


@responses.activate
def test_fetch_breweries_to_path_retries_on_503_then_succeeds(tmp_path):
    """Retries on 503 (server error) and then succeeds."""
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries/meta",
        json={"total": 2},
    )
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=200",
        status=503,
    )
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=200",
        json=[{"id": "x"}, {"id": "y"}],
    )
    out = tmp_path / "out.jsonl"

    path, total = fetch_breweries_to_path(out, max_retries=2, retry_delay=0.01)

    assert total == 2
    assert len(path.read_text().strip().split("\n")) == 2
    assert len([c for c in responses.calls if "breweries?page=1" in c.request.url]) == 2
