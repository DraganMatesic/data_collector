from typing import Optional
from data_collector.settings.main import (GeneralSettings,
                                          MainDatabaseSettings)

# Always load general settings
general_settings = GeneralSettings()

# Optional country-specific settings
example_settings: Optional[object] = None

# Dynamically import country configs if present
# try:
#     from data_collector.settings.example import ExampleSettings
#     example_settings : ExampleSettings = ExampleSettings()
# except ModuleNotFoundError:
#     pass
#
