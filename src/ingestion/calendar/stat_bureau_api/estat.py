"""Fetch and parse e-Stat JSON values for Statistics Bureau indicators."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any

import requests

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, StatBureauIndicatorSpec
from .parser import (
    ESTAT_STATS_DATA_URL,
    PROVIDER,
    StatBureauCalendarEventRecord,
    StatBureauCalendarRawRecord,
)


class StatBureauValueParseError(ValueError):
    """Raised when an e-Stat JSON response has an unexpected shape."""


@dataclass(frozen=True)
class EStatValue:
    """One parsed e-Stat scalar value."""

    indicator: str
    reference_date: date
    reference_label: str
    stats_data_id: str
    time_code: str
    actual: str
    unit: str
    attrs: dict[str, str]
    source_url: str


def time_code_for_month(reference: date) -> str:
    """Return the e-Stat monthly time code for a reference month."""
    return f"{reference.year}00{reference.month:02d}{reference.month:02d}"


def _reference_label(reference: date) -> str:
    return reference.strftime("%B %Y")


def _resolve_app_id(app_id: str | None) -> str:
    resolved = (app_id or os.getenv("ESTAT_APP_ID") or "").strip()
    if not resolved:
        raise RuntimeError("ESTAT_APP_ID not set")
    return resolved


def fetch_estat_value_json(
    spec: StatBureauIndicatorSpec,
    reference: date,
    *,
    app_id: str | None = None,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """GET one scalar e-Stat value and return decoded JSON."""
    client = session or requests.Session()
    params = {
        "appId": _resolve_app_id(app_id),
        "lang": "E",
        "statsDataId": spec.stats_data_id,
        "cdTime": time_code_for_month(reference),
    }
    params.update(spec.estat_params)
    response = client.get(
        ESTAT_STATS_DATA_URL,
        params=params,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def _get_stats_data(data: dict[str, Any]) -> dict[str, Any]:
    root = data.get("GET_STATS_DATA")
    if not isinstance(root, dict):
        raise StatBureauValueParseError("e-Stat response lacks GET_STATS_DATA")
    result = root.get("RESULT") or {}
    status = str(result.get("STATUS", "0"))
    if status not in {"0", "00"}:
        message = result.get("ERROR_MSG") or result.get("ERROR_MSG_JP") or status
        raise StatBureauValueParseError(f"e-Stat API error: {message}")
    stats = root.get("STATISTICAL_DATA")
    if not isinstance(stats, dict):
        raise StatBureauValueParseError(
            "e-Stat response lacks STATISTICAL_DATA"
        )
    return stats


def _extract_values(stats: dict[str, Any]) -> list[dict[str, Any]]:
    data_inf = stats.get("DATA_INF")
    if not isinstance(data_inf, dict):
        raise StatBureauValueParseError("e-Stat response lacks DATA_INF")
    values = data_inf.get("VALUE")
    if isinstance(values, list):
        return [v for v in values if isinstance(v, dict)]
    if isinstance(values, dict):
        return [values]
    raise StatBureauValueParseError("e-Stat response lacks VALUE")


def _attrs_for_value(value: dict[str, Any]) -> dict[str, str]:
    return {
        str(k)[1:]: str(v)
        for k, v in value.items()
        if str(k).startswith("@")
    }


def _param_attr_name(param_name: str) -> str:
    if not param_name.startswith("cd"):
        return param_name
    rest = param_name[2:]
    return rest[:1].lower() + rest[1:]


def parse_estat_value_json(
    data: dict[str, Any],
    *,
    indicator: str,
    reference: date,
) -> EStatValue:
    """Extract and validate one scalar e-Stat value from decoded JSON."""
    spec = INDICATOR_REGISTRY[indicator]
    stats = _get_stats_data(data)
    expected_time = time_code_for_month(reference)
    expected_attrs = {
        _param_attr_name(k): v
        for k, v in spec.estat_params.items()
    }
    expected_attrs["time"] = expected_time

    matching: list[tuple[dict[str, Any], dict[str, str]]] = []
    for value in _extract_values(stats):
        attrs = _attrs_for_value(value)
        if all(attrs.get(k) == expected for k, expected in expected_attrs.items()):
            matching.append((value, attrs))
    if not matching:
        raise StatBureauValueParseError(
            f"e-Stat response lacks value for {indicator} {expected_time}"
        )
    value, attrs = matching[0]
    actual = str(value.get("$", "")).strip()
    if not actual:
        raise StatBureauValueParseError(
            f"e-Stat value is blank for {indicator} {expected_time}"
        )
    return EStatValue(
        indicator=indicator,
        reference_date=reference,
        reference_label=_reference_label(reference),
        stats_data_id=spec.stats_data_id,
        time_code=expected_time,
        actual=actual,
        unit=attrs.get("unit", spec.unit),
        attrs=attrs,
        source_url=spec.source_url,
    )


def _record_id(indicator: str, reference: date) -> str:
    spec = INDICATOR_REGISTRY[indicator]
    return synthesize_event_id(
        PROVIDER,
        spec.country_code,
        canonicalize_indicator(spec.title),
        reference.isoformat(),
    )


def estat_value_to_records(
    value: EStatValue,
    *,
    snapshot_epoch_ms: int,
    event_time_utc: str = "",
) -> tuple[StatBureauCalendarRawRecord, StatBureauCalendarEventRecord]:
    """Convert a parsed e-Stat value into raw + event records."""
    spec = INDICATOR_REGISTRY[value.indicator]
    provider_event_id = _record_id(value.indicator, value.reference_date)
    precision = "datetime"
    if not event_time_utc:
        event_time_utc = datetime.combine(
            value.reference_date,
            time.min,
            tzinfo=timezone.utc,
        ).isoformat()
        precision = "approximate"

    payload = {
        "provider": PROVIDER,
        "provider_event_id": provider_event_id,
        "kind": "estat_value",
        "indicator": value.indicator,
        "title": spec.title,
        "stats_data_id": value.stats_data_id,
        "time_code": value.time_code,
        "estat_params": dict(spec.estat_params),
        "value_attrs": value.attrs,
        "reference_date": value.reference_date.isoformat(),
        "event_time_utc": event_time_utc,
        "actual": value.actual,
        "unit": value.unit,
        "source_url": value.source_url,
    }
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    content_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw = StatBureauCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event = StatBureauCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision=precision,
        reference_date=value.reference_date.isoformat(),
        reference_label=value.reference_label,
        country_code=spec.country_code,
        indicator_id=None,
        category=spec.category,
        title=spec.title,
        importance=spec.importance,
        currency="",
        unit=spec.unit,
        actual=value.actual,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="e-Stat",
        source_url=value.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw, event
