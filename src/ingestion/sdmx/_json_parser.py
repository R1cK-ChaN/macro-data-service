"""Shared SDMX-JSON observation parser.

Handles the ``dataSets[0].series.{key}.observations.{idx}`` pattern used
by ECB, Eurostat, UNSD, ILO, and (with slight variation) IMF.
"""

from __future__ import annotations

from typing import Callable

from ._parsing import normalize_date
from ._types import SDMXObservation


def parse_sdmx_json_observations(
    data: dict,
    *,
    series_id: str,
    dataflow: str,
    limit: int,
    normalize_date_fn: Callable[[str], str] | None = None,
) -> list[SDMXObservation]:
    """Parse an SDMX-JSON data response into observations.

    Works for both ``"structure"`` (ECB) and ``"structures"`` (standard)
    response keys — tries both.
    """
    if normalize_date_fn is None:
        normalize_date_fn = normalize_date

    observations: list[SDMXObservation] = []

    try:
        datasets = data["dataSets"]
        # ECB uses "structure" (singular), standard uses "structures"
        structure = data.get("structure") or (data.get("structures") or [None])[0]
    except (KeyError, TypeError, IndexError):
        return observations

    if not datasets or not structure:
        return observations

    # Find TIME_PERIOD dimension in observation dimensions
    obs_dims = structure.get("dimensions", {}).get("observation", [])
    time_dim = None
    for dim in obs_dims:
        if dim.get("id") == "TIME_PERIOD":
            time_dim = dim
            break

    if time_dim is None:
        return observations

    # Build index → time period mapping
    time_map: dict[str, str] = {}
    for i, val in enumerate(time_dim.get("values", [])):
        time_map[str(i)] = val.get("id", val.get("value", ""))

    # Iterate all series keys in the first dataset
    all_series = datasets[0].get("series", {})
    for _series_key, series_data in all_series.items():
        for obs_idx, obs_array in series_data.get("observations", {}).items():
            period = time_map.get(obs_idx)
            if not period:
                continue
            value = obs_array[0] if obs_array else None
            if value is None:
                continue
            try:
                observations.append(SDMXObservation(
                    series_id=series_id,
                    date=normalize_date_fn(period),
                    value=float(value),
                    dataflow=dataflow,
                ))
            except (ValueError, TypeError):
                continue

    observations.sort(key=lambda o: o.date, reverse=True)
    return observations[:limit] if limit else observations
