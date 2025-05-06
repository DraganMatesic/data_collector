from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path
import os

class DefaultDatabaseSettings(BaseSettings):
    password: str = Field(..., alias="DB_PASS")



class Settings(BaseSettings):
    default_db: DefaultDatabaseSettings = DefaultDatabaseSettings()


settings = Settings()

print(settings.database.password)