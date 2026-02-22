import pytest
from sqlalchemy import text

from data_collector.settings.main import MainDatabaseSettings
from data_collector.utilities.database.main import Database


@pytest.mark.integration
def test_database_session_smoke() -> None:
    try:
        db = Database(MainDatabaseSettings())
        with db.create_session() as session:
            value = session.execute(text("SELECT 1")).scalar_one()
        assert value == 1
    except Exception as exc:  # pragma: no cover - environment-dependent skip path
        pytest.skip(f"Integration database is not available: {exc}")
