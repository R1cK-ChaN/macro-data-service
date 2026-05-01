from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.series_config import MACRO_SERIES
from ingestion.source_capabilities import SourceCapabilityManager
from storage.sqlite import SQLiteEngineStore


def test_workbook_fred_series_config_includes_wei_and_hvs() -> None:
    expected = {
        "WEI": ("Weekly Economic Index", "growth", "weekly"),
        "RHORUSQ156N": ("Homeownership Rate", "housing", "quarterly"),
        "RRVRUSQ156N": ("Rental Vacancy Rate", "housing", "quarterly"),
        "RHVRUSQ156N": ("Homeowner Vacancy Rate", "housing", "quarterly"),
    }

    for series_id, (name, category, freq) in expected.items():
        assert MACRO_SERIES[series_id] == {
            "name": name,
            "category": category,
            "freq": freq,
        }


def test_workbook_fred_series_seed_families_and_concepts(tmp_path: Path) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    store.seed_obs_sources_and_families()
    store.seed_concept_map()

    expected = {
        "WEI_US": ("WEI", "us.growth.weekly_economic_index", "percent"),
        "HOMEOWNERSHIP_RATE_US": (
            "RHORUSQ156N", "us.housing.homeownership_rate", "percent",
        ),
        "RENTAL_VACANCY_RATE_US": (
            "RRVRUSQ156N", "us.housing.rental_vacancy_rate", "percent",
        ),
        "HOMEOWNER_VACANCY_RATE_US": (
            "RHVRUSQ156N", "us.housing.homeowner_vacancy_rate", "percent",
        ),
    }

    for concept_id, (series_id, family_id, unit) in expected.items():
        family = store.get_obs_family(family_id)
        assert family is not None
        assert family.source_id == "fred"
        assert family.provider_series_id == series_id
        assert family.unit == unit

        mappings = store.get_concept_series(concept_id)
        assert len(mappings) == 1
        assert mappings[0].source_id == "fred"
        assert mappings[0].provider_series_id == series_id
        assert mappings[0].obs_family_id == family_id


def test_fred_source_discovery_surfaces_workbook_wei_and_hvs(tmp_path: Path) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    manager = SourceCapabilityManager(store)

    ids = {
        entity["entity_id"]
        for entity in manager.list_entities("fred", query="vacancy", limit=20)["entities"]
    }
    assert {"RRVRUSQ156N", "RHVRUSQ156N"} <= ids

    wei = manager.list_entities(
        "fred", query="Weekly Economic Index", limit=5, refresh=True,
    )
    assert [entity["entity_id"] for entity in wei["entities"]] == ["WEI"]
