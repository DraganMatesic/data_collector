"""Pydantic settings for proxy provider credentials, parameterized by BrightData zone."""

from __future__ import annotations

import os

from pydantic_settings import BaseSettings, SettingsConfigDict

from data_collector.proxy.models import ProxyData


class ProxySettings(BaseSettings):
    """Proxy provider credentials parameterized by BrightData zone.

    Each zone (mapped to an app_parent) has its own set of environment variables
    following the pattern ``DC_PROXY_{FIELD}_{ZONE}``:

        DC_PROXY_HOST_{ZONE}, DC_PROXY_PORT_{ZONE},
        DC_PROXY_USERNAME_{ZONE}, DC_PROXY_PASSWORD_{ZONE},
        DC_PROXY_COUNTRY_{ZONE} (optional), DC_PROXY_PROTOCOL_{ZONE} (optional)

    The zone suffix corresponds to the uppercase app_parent name. For example,
    apps under ``data_collector/examples/scraping/`` use zone ``SCRAPING``,
    while apps under ``data_collector/croatia/registry/`` use zone ``REGISTRY``.

    This design allows each BrightData zone to have independent credentials,
    enabling per-zone expense tracking on the provider dashboard.

    Examples:
        From zone-specific environment variables::

            settings = ProxySettings.from_zone("scraping")
            proxy_data = settings.to_proxy_data()

        Direct construction (testing, overrides)::

            settings = ProxySettings(
                host="brd.superproxy.io",
                port=22225,
                username="brd-customer-xxx-zone-residential_hr",
                password="secret",
                country="hr",
            )
    """

    model_config = SettingsConfigDict(populate_by_name=True)

    host: str
    port: int
    username: str
    password: str
    country: str | None = None
    protocol: str = "http"

    def to_proxy_data(self) -> ProxyData:
        """Convert to a ProxyData dataclass for ProxyProvider and ProxyManager.

        Returns:
            Frozen ProxyData instance with all fields mapped from settings.
        """
        return ProxyData(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            country=self.country,
            protocol=self.protocol,
        )

    @classmethod
    def from_zone(cls, zone: str) -> ProxySettings:
        """Create settings from zone-specific environment variables.

        Reads ``DC_PROXY_{FIELD}_{ZONE}`` for each field. Required fields
        (host, port, username, password) must be present or Pydantic raises
        ``ValidationError``. Optional fields (country, protocol) fall back
        to their defaults when the env var is absent.

        Args:
            zone: BrightData zone suffix matching the app_parent name
                (e.g., "scraping", "registry"). Case-insensitive.

        Returns:
            ProxySettings instance populated from environment variables.

        Raises:
            pydantic.ValidationError: If required environment variables are missing.
        """
        suffix = zone.upper()
        values: dict[str, str | int] = {}
        for field_name in ("HOST", "PORT", "USERNAME", "PASSWORD", "COUNTRY", "PROTOCOL"):
            env_var = f"DC_PROXY_{field_name}_{suffix}"
            value = os.environ.get(env_var)
            if value is not None:
                values[field_name.lower()] = value
        return cls(**values)  # type: ignore[arg-type]
