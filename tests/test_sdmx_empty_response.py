"""Issue #46 — SDMX base client tolerates empty response bodies.

ECB returns HTTP 200 with a zero-byte body when a query matches no
observations (live-probed 2026-04-26 with
``EXR/D.USD.EUR.SP00.A?startPeriod=1900-01-01&endPeriod=1900-01-02``
→ status=200, size=0). Before this fix, ``response.json()`` blew up
at byte 0 and the connector tripped its failure breaker.

These tests pin the regression at every call site that decodes JSON
in the base client: ``_parse_data_response`` (the value-sweep path
that surfaced the production failure), ``list_dataflows``,
``get_datastructure``, and ``estimate_size``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.timeseries.sdmx._base_client import SDMXClient
from ingestion.timeseries.sdmx._config import ECB_CONFIG
from ingestion.timeseries.sdmx._errors import SDMXAPIError


def _fake_response(
    *, status_code: int = 200, content: bytes = b"", url: str = "http://x",
) -> requests.Response:
    r = requests.Response()
    r.status_code = status_code
    r._content = content
    r.url = url
    return r


@pytest.fixture()
def client() -> SDMXClient:
    c = SDMXClient(ECB_CONFIG, timeout=5)
    c.config = ECB_CONFIG  # ensure provider_name visible
    return c


def test_response_json_returns_empty_dict_on_zero_byte_body(client: SDMXClient) -> None:
    response = _fake_response(content=b"")
    assert client._response_json(response, context="EXR/.") == {}


def test_response_json_returns_empty_dict_on_204(client: SDMXClient) -> None:
    response = _fake_response(status_code=204, content=b"")
    assert client._response_json(response) == {}


def test_response_json_decodes_normal_body(client: SDMXClient) -> None:
    response = _fake_response(content=b'{"data": {"dataflows": []}}')
    assert client._response_json(response) == {"data": {"dataflows": []}}


def test_get_data_yields_empty_observations_on_empty_body(client: SDMXClient) -> None:
    """Production failure path: value sweep against an ECB series whose
    upstream returned 200 with no body. Should produce zero observations,
    not raise JSONDecodeError.
    """
    empty = _fake_response(content=b"")
    with patch.object(client, "_get", return_value=empty):
        observations = client.get_data(
            "EXR", "D.USD.EUR.SP00.A", series_id="EXR", limit=1,
        )
    assert observations == []


def test_list_dataflows_raises_on_empty_body(client: SDMXClient) -> None:
    """A catalog endpoint returning an empty body is an outage, not a
    "no data" signal — raising prevents `refresh_catalog()` from silently
    reporting a zero-flow success on a transient catalog response."""
    empty = _fake_response(content=b"")
    with patch.object(client, "_get", return_value=empty):
        with pytest.raises(SDMXAPIError, match="empty body"):
            client.list_dataflows()


def test_get_datastructure_raises_on_empty_body(client: SDMXClient) -> None:
    """Empty body for a structure request still has to raise — there is
    no DSD to return — but with the connector-friendly ``SDMXAPIError``
    rather than a raw ``JSONDecodeError``."""
    empty = _fake_response(content=b"")
    with patch.object(client, "_get", return_value=empty):
        with pytest.raises(SDMXAPIError):
            client.get_datastructure("EXR", version="1.0")


def test_estimate_size_yields_zero_on_empty_body(client: SDMXClient) -> None:
    empty = _fake_response(content=b"")
    with patch.object(client, "_get", return_value=empty), \
         patch.object(client, "get_datastructure", side_effect=SDMXAPIError("x")):
        estimate = client.estimate_size("EXR")
    assert estimate.total_series == 0


def test_estimate_size_skips_codelist_fallback_on_empty_probe(
    client: SDMXClient,
) -> None:
    """An explicit zero-observation response must not fall back to the
    codelist-product size — that would advertise a fake nonzero estimate
    for a dataflow whose upstream truly returned no data.
    """
    empty = _fake_response(content=b"")
    with patch.object(client, "_get", return_value=empty), \
         patch.object(client, "get_datastructure") as ds_mock:
        estimate = client.estimate_size("EXR")
    assert estimate.total_series == 0
    ds_mock.assert_not_called()


def test_list_dataflows_does_not_cache_failed_catalog(client: SDMXClient) -> None:
    """A transient empty-body response on the catalog endpoint raises and
    must not be cached — the next caller must get a fresh attempt."""
    empty = _fake_response(content=b"")
    populated = _fake_response(
        content=b'{"data": {"dataflows": [{"id": "EXR", "agencyID": "ECB"}]}}',
    )
    with patch.object(client, "_get", side_effect=[empty, populated]):
        with pytest.raises(SDMXAPIError):
            client.list_dataflows()
        second = client.list_dataflows()
    assert len(second) == 1
    assert second[0].id == "EXR"
