"""Integration tests for Database.query() wrapper."""

from typing import Any

import pytest
from sqlalchemy.orm import Query, Session

from data_collector.tables.examples import ExampleTable
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions import runtime


def make_row(person_id: int, company_id: int = 1) -> ExampleTable:
    """Create a single ExampleTable instance with a computed sha."""
    data: dict[str, Any] = {"company_id": company_id, "person_id": person_id}
    sha = str(runtime.make_hash(data))
    return ExampleTable(company_id=company_id, person_id=person_id, name=f"P{person_id}", sha=sha)


@pytest.mark.integration
class TestQuery:
    def test_returns_query_object(self, db: Database, session: Session) -> None:
        q = db.query(session, ExampleTable)
        assert isinstance(q, Query)

    @pytest.mark.usefixtures("clean_example_table")
    def test_returns_inserted_rows(self, db: Database, session: Session) -> None:
        rows = [make_row(i) for i in range(1, 4)]
        db.bulk_insert(rows, session=session)
        session.commit()

        result = db.query(session, ExampleTable).all()
        assert len(result) == 3

    @pytest.mark.usefixtures("clean_example_table")
    def test_filter_chaining(self, db: Database, session: Session) -> None:
        rows = [make_row(1, company_id=1), make_row(2, company_id=1), make_row(3, company_id=2)]
        db.bulk_insert(rows, session=session)
        session.commit()

        result = db.query(session, ExampleTable).filter(ExampleTable.company_id == 1).all()
        assert len(result) == 2
        assert all(r.company_id == 1 for r in result)
