"""Integration tests for Database.merge() — the core synchronization method."""

from datetime import datetime
from typing import Any, cast

import pytest
from sqlalchemy.orm import Session

from data_collector.tables.examples import ExampleTable
from data_collector.utilities.database.main import Database, Stats
from data_collector.utilities.functions import runtime


def make_example_rows(
    count: int,
    company_id: int = 1,
    start_person_id: int = 1,
) -> list[ExampleTable]:
    """Create ExampleTable instances with unique sha values."""
    rows: list[ExampleTable] = []
    for i in range(count):
        pid = start_person_id + i
        data: dict[str, Any] = {
            "company_id": company_id,
            "person_id": pid,
            "name": f"Name{pid}",
            "surname": f"Surname{pid}",
        }
        sha = str(runtime.make_hash(data))
        rows.append(ExampleTable(
            company_id=company_id,
            person_id=pid,
            name=f"Name{pid}",
            surname=f"Surname{pid}",
            sha=sha,
        ))
    return rows


@pytest.mark.integration
@pytest.mark.usefixtures("clean_example_table")
class TestMergeInsert:
    def test_insert_into_empty_table(self, db: Database, session: Session) -> None:
        rows = make_example_rows(3)
        db.merge(rows, session)

        result = session.query(ExampleTable).all()
        assert len(result) == 3
        assert all(cast(Any, r.archive) is None for r in result)

    def test_idempotent_reinsert(self, db: Database, session: Session) -> None:
        rows = make_example_rows(3)
        db.merge(rows, session)

        # Re-merge with identical rows — nothing should change
        rows_again = make_example_rows(3)
        db.merge(rows_again, session)

        assert session.query(ExampleTable).count() == 3


@pytest.mark.integration
@pytest.mark.usefixtures("clean_example_table")
class TestMergeArchive:
    def test_archives_removed_records(self, db: Database, session: Session) -> None:
        rows_abc = make_example_rows(3)
        db.merge(rows_abc, session)

        # Merge with only first 2 — third should be archived
        rows_ab = make_example_rows(2)
        db.merge(rows_ab, session)

        all_rows = session.query(ExampleTable).all()
        assert len(all_rows) == 3

        archived = [r for r in all_rows if cast(Any, r.archive) is not None]
        active = [r for r in all_rows if cast(Any, r.archive) is None]
        assert len(archived) == 1
        assert len(active) == 2
        assert cast(Any, archived[0].person_id) == 3

    def test_archives_with_custom_date(self, db: Database, session: Session) -> None:
        rows = make_example_rows(2)
        db.merge(rows, session)

        fixed_date = datetime(2024, 1, 15, 12, 0, 0)
        rows_first_only = make_example_rows(1)
        db.merge(rows_first_only, session, archive_date=fixed_date)

        archived_row = session.query(ExampleTable).filter(
            ExampleTable.archive.isnot(None),
        ).one()
        assert cast(Any, archived_row.archive) == fixed_date

    def test_no_archive_when_update_false(self, db: Database, session: Session) -> None:
        rows = make_example_rows(2)
        db.merge(rows, session)

        rows_first_only = make_example_rows(1)
        db.merge(rows_first_only, session, update=False, delete=False)

        all_rows = session.query(ExampleTable).all()
        assert len(all_rows) == 2
        assert all(cast(Any, r.archive) is None for r in all_rows)


@pytest.mark.integration
@pytest.mark.usefixtures("clean_example_table")
class TestMergeDelete:
    def test_deletes_when_delete_true(self, db: Database, session: Session) -> None:
        rows = make_example_rows(3)
        db.merge(rows, session)

        rows_first_only = make_example_rows(1)
        db.merge(rows_first_only, session, delete=True, update=False)

        assert session.query(ExampleTable).count() == 1


@pytest.mark.integration
class TestMergeStats:
    @pytest.mark.usefixtures("clean_example_table")
    def test_returns_stats(self, db: Database, session: Session) -> None:
        # Seed with A, B
        rows_ab = make_example_rows(2)
        db.merge(rows_ab, session)

        # Merge with B, C — A should be archived, C inserted
        row_b = make_example_rows(1, start_person_id=2)
        row_c = make_example_rows(1, start_person_id=3)
        result = db.merge(row_b + row_c, session, stats=True)

        assert isinstance(result, Stats)
        assert result.inserted == 1
        assert result.archived == 1
        assert result.deleted == 0
        assert result.number_of_records == 2

    @pytest.mark.usefixtures("clean_example_table")
    def test_returns_none_without_stats(self, db: Database, session: Session) -> None:
        rows = make_example_rows(1)
        result = db.merge(rows, session, stats=False)
        assert result is None

    def test_empty_input_with_stats(self, db: Database, session: Session) -> None:
        result = db.merge([], session, stats=True)
        assert isinstance(result, Stats)
        assert result.inserted == 0
        assert result.archived == 0
        assert result.deleted == 0

    def test_empty_input_returns_none(self, db: Database, session: Session) -> None:
        result = db.merge([], session, stats=False)
        assert result is None


@pytest.mark.integration
@pytest.mark.usefixtures("clean_example_table")
class TestMergeFilters:
    def test_filter_scoped_merge(self, db: Database, session: Session) -> None:
        # Insert 3 rows for company_id=1 and 2 rows for company_id=2
        rows_cid1 = make_example_rows(3, company_id=1)
        rows_cid2 = make_example_rows(2, company_id=2)
        db.merge(rows_cid1 + rows_cid2, session)
        assert session.query(ExampleTable).count() == 5

        # Merge only 1 row for company_id=1, scoped by filter
        rows_cid1_partial = make_example_rows(1, company_id=1)
        db.merge(
            rows_cid1_partial,
            session,
            filters=(ExampleTable.company_id == 1),
        )

        # company_id=1: 1 active + 2 archived
        cid1_rows = session.query(ExampleTable).filter(
            ExampleTable.company_id == 1,
        ).all()
        archived_cid1 = [r for r in cid1_rows if cast(Any, r.archive) is not None]
        assert len(archived_cid1) == 2

        # company_id=2: untouched
        cid2_rows = session.query(ExampleTable).filter(
            ExampleTable.company_id == 2,
        ).all()
        assert all(cast(Any, r.archive) is None for r in cid2_rows)
        assert len(cid2_rows) == 2


@pytest.mark.integration
class TestMergeGuards:
    def test_rejects_list_of_strings(self, db: Database, session: Session) -> None:
        with pytest.raises(TypeError, match="merge.*received"):
            db.merge(["hash1", "hash2"], session)
