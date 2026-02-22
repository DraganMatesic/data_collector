import os

import pytest
from sqlalchemy import text

from data_collector.settings.main import MainDatabaseSettings
from data_collector.utilities.database.main import Database


@pytest.mark.integration
def test_database_session_smoke() -> None:
    required_env = (
        "DC_DB_MAIN_USERNAME",
        "DC_DB_MAIN_PASSWORD",
        "DC_DB_MAIN_DATABASENAME",
        "DC_DB_MAIN_IP",
        "DC_DB_MAIN_PORT",
    )
    # MainDatabaseSettings reads directly from environment variables, so validate
    # presence before attempting a connection for local opt-in runs.
    missing = [env_name for env_name in required_env if not os.environ.get(env_name)]
    if missing:
        pytest.skip(f"Missing required database env vars: {', '.join(missing)}")

    db = Database(MainDatabaseSettings())
    with db.create_session() as session:
        value = session.execute(text("SELECT 1")).scalar_one()
    assert value == 1
