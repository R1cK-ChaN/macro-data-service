"""Provider configuration for SDMX clients."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SDMXProviderConfig:
    """Declarative configuration for an SDMX data provider."""

    provider_name: str
    base_url: str
    default_agency: str
    data_format: str = "json"       # "json" | "csv"
    request_delay: float = 0.5      # seconds between requests
    requires_auth: bool = False
    auth_header: str = ""
    auth_env_var: str = ""
    default_version: str = "1.0"
    default_all_key: str = "."      # "all" for IMF/UNSD


BIS_CONFIG = SDMXProviderConfig(
    provider_name="BIS",
    base_url="https://stats.bis.org/api/v2",
    default_agency="BIS",
    data_format="csv",
)

IMF_CONFIG = SDMXProviderConfig(
    provider_name="IMF",
    base_url="https://api.imf.org/external/sdmx/3.0",
    default_agency="IMF.STA",
    request_delay=1.0,
    requires_auth=True,
    auth_header="Ocp-Apim-Subscription-Key",
    auth_env_var="IMF_API_KEY",
    default_all_key="all",
)

EUROSTAT_CONFIG = SDMXProviderConfig(
    provider_name="Eurostat",
    base_url="https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1",
    default_agency="ESTAT",
)

ECB_CONFIG = SDMXProviderConfig(
    provider_name="ECB",
    base_url="https://data-api.ecb.europa.eu/service",
    default_agency="ECB",
)

UNSD_CONFIG = SDMXProviderConfig(
    provider_name="UNSD",
    base_url="https://data.un.org/ws/rest",
    default_agency="UNSD",
    request_delay=1.0,
    default_all_key="all",
)

ILO_CONFIG = SDMXProviderConfig(
    provider_name="ILO",
    base_url="https://sdmx.ilo.org/rest",
    default_agency="ILO",
)

OECD_CONFIG = SDMXProviderConfig(
    provider_name="OECD",
    base_url="https://sdmx.oecd.org/public/rest",
    default_agency="OECD.SDD.STES",
)

BUNDESBANK_CONFIG = SDMXProviderConfig(
    provider_name="Bundesbank",
    base_url="https://api.statistiken.bundesbank.de/rest",
    default_agency="BBK",
)
