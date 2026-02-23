"""Integration tests for Database CRUD operations: bulk_insert, delete, archive, update_insert."""

from datetime import datetime
from typing import Any, cast

import pytest
from sqlalchemy.orm import Session

from data_collector.tables.examples import ExampleTable
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions import runtime


def make_row(person_id: int, company_id: int = 1, name: str = "Test") -> ExampleTable:
    """Create a single ExampleTable instance with a computed sha."""
    data: dict[str, Any] = {
        "company_id": company_id,
        "person_id": person_id,
        "name": name,
    }
    sha = str(runtime.make_hash(data))
    return ExampleTable(
        company_id=company_id,
        person_id=person_id,
        name=name,
        sha=sha,
    )


@pytest.mark.integration
@pytest.mark.usefixtures("clean_example_table")
class TestBulkInsert:
    def test_persists_rows(self, db: Database, session: Session) -> None:
        rows = [make_row(i) for i in range(1, 4)]
        db.bulk_insert(rows, session=session)
        session.commit()

        assert session.query(ExampleTable).count() == 3

    def test_empty_list_is_noop(self, db: Database, session: Session) -> None:
        db.bulk_insert([], session=session)
        session.commit()
        assert session.query(ExampleTable).count() == 0


@pytest.mark.integration
class TestDelete:
    @pytest.mark.usefixtures("clean_example_table")
    def test_removes_objects(self, db: Database, session: Session) -> None:
        rows = [make_row(i) for i in range(1, 4)]
        db.bulk_insert(rows, session=session)
        session.commit()

        loaded = session.query(ExampleTable).all()
        db.delete(loaded[:2], session)
        session.commit()

        assert session.query(ExampleTable).count() == 1

    def test_empty_list_is_noop(self, db: Database, session: Session) -> None:
        db.delete([], session)
        # No exception is success


@pytest.mark.integration
class TestArchive:
    @pytest.mark.usefixtures("clean_example_table")
    def test_sets_timestamp(self, db: Database, session: Session) -> None:
        rows = [make_row(1), make_row(2)]
        db.bulk_insert(rows, session=session)
        session.commit()

        loaded = session.query(ExampleTable).all()
        db.archive([loaded[0]], session=session)
        session.commit()

        refreshed = session.query(ExampleTable).order_by(ExampleTable.person_id).all()
        assert isinstance(cast(Any, refreshed[0].archive), datetime)
        assert cast(Any, refreshed[1].archive) is None

    @pytest.mark.usefixtures("clean_example_table")
    def test_uses_custom_date(self, db: Database, session: Session) -> None:
        row = make_row(1)
        db.bulk_insert([row], session=session)
        session.commit()

        loaded = session.query(ExampleTable).all()
        fixed_date = datetime(2025, 6, 1, 0, 0, 0)
        db.archive(loaded, session=session, archive_date=fixed_date)
        session.commit()

        refreshed = session.query(ExampleTable).one()
        assert cast(Any, refreshed.archive) == fixed_date

    def test_empty_list_is_noop(self, db: Database, session: Session) -> None:
        db.archive([], session=session)
        # No exception is success


@pytest.mark.integration
@pytest.mark.usefixtures("clean_example_table")
class TestUpdateInsert:
    def test_inserts_new_rows(self, db: Database, session: Session) -> None:
        rows = [make_row(1), make_row(2)]
        stats = db.update_insert(rows, session, filter_cols=["company_id", "person_id"])

        assert stats.inserted == 2
        assert stats.updated == 0
        assert session.query(ExampleTable).count() == 2

    def test_updates_existing_row(self, db: Database, session: Session) -> None:
        original = make_row(1, name="Alice")
        db.update_insert(original, session, filter_cols=["company_id", "person_id"])

        updated = make_row(1, name="Bob")
        stats = db.update_insert(updated, session, filter_cols=["company_id", "person_id"])

        assert stats.updated == 1
        assert stats.inserted == 0

        row = session.query(ExampleTable).filter(ExampleTable.person_id == 1).one()
        assert cast(Any, row.name) == "Bob"

    def test_no_change_when_identical(self, db: Database, session: Session) -> None:
        row = make_row(1, name="Alice")
        db.update_insert(row, session, filter_cols=["company_id", "person_id"])

        same_row = make_row(1, name="Alice")
        stats = db.update_insert(same_row, session, filter_cols=["company_id", "person_id"])

        assert stats.updated == 0
        assert stats.inserted == 0
