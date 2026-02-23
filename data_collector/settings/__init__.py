"""Settings package exports and default settings objects."""

from __future__ import annotations

from data_collector.settings.main import GeneralSettings, MainDatabaseSettings

# Always load general settings
general_settings = GeneralSettings()

# Optional country-specific settings
EXAMPLE_SETTINGS: object | None = None

__all__ = ["EXAMPLE_SETTINGS", "MainDatabaseSettings", "general_settings"]

# Dynamically import country configs if present
# try:
#     from data_collector.settings.example import ExampleSettings
#     EXAMPLE_SETTINGS : ExampleSettings = ExampleSettings()
# except ModuleNotFoundError:
#     pass
#
