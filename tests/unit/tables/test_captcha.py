"""Tests for captcha codebook tables, CaptchaLog, and CaptchaLogError ORM table structure."""

from sqlalchemy import BigInteger, Boolean, DateTime, Float, String, Text

from data_collector.tables.captcha import (
    CaptchaLog,
    CaptchaLogError,
    CodebookCaptchaErrorCategory,
    CodebookCaptchaSolveStatus,
)

# ---------------------------------------------------------------------------
# CodebookCaptchaSolveStatus
# ---------------------------------------------------------------------------


class TestCodebookCaptchaSolveStatus:
    """Verify CodebookCaptchaSolveStatus codebook table structure."""

    def test_tablename(self) -> None:
        assert CodebookCaptchaSolveStatus.__tablename__ == "c_captcha_solve_status"

    def test_id_column_is_biginteger_primary_key(self) -> None:
        column = CodebookCaptchaSolveStatus.__table__.columns["id"]
        assert column.primary_key
        assert isinstance(column.type, BigInteger)

    def test_description_column(self) -> None:
        column = CodebookCaptchaSolveStatus.__table__.columns["description"]
        assert isinstance(column.type, String)
        assert column.type.length == 128  # pyright: ignore[reportOptionalMemberAccess]

    def test_sha_column(self) -> None:
        column = CodebookCaptchaSolveStatus.__table__.columns["sha"]
        assert isinstance(column.type, String)
        assert column.type.length == 64  # pyright: ignore[reportOptionalMemberAccess]

    def test_archive_column(self) -> None:
        column = CodebookCaptchaSolveStatus.__table__.columns["archive"]
        assert isinstance(column.type, DateTime)

    def test_date_created_column(self) -> None:
        column = CodebookCaptchaSolveStatus.__table__.columns["date_created"]
        assert isinstance(column.type, DateTime)
        assert column.server_default is not None

    def test_column_count(self) -> None:
        assert len(CodebookCaptchaSolveStatus.__table__.columns) == 5


# ---------------------------------------------------------------------------
# CodebookCaptchaErrorCategory
# ---------------------------------------------------------------------------


class TestCodebookCaptchaErrorCategory:
    """Verify CodebookCaptchaErrorCategory codebook table structure."""

    def test_tablename(self) -> None:
        assert CodebookCaptchaErrorCategory.__tablename__ == "c_captcha_error_category"

    def test_id_column_is_biginteger_primary_key(self) -> None:
        column = CodebookCaptchaErrorCategory.__table__.columns["id"]
        assert column.primary_key
        assert isinstance(column.type, BigInteger)

    def test_description_column(self) -> None:
        column = CodebookCaptchaErrorCategory.__table__.columns["description"]
        assert isinstance(column.type, String)
        assert column.type.length == 128  # pyright: ignore[reportOptionalMemberAccess]

    def test_sha_column(self) -> None:
        column = CodebookCaptchaErrorCategory.__table__.columns["sha"]
        assert isinstance(column.type, String)
        assert column.type.length == 64  # pyright: ignore[reportOptionalMemberAccess]

    def test_archive_column(self) -> None:
        column = CodebookCaptchaErrorCategory.__table__.columns["archive"]
        assert isinstance(column.type, DateTime)

    def test_date_created_column(self) -> None:
        column = CodebookCaptchaErrorCategory.__table__.columns["date_created"]
        assert isinstance(column.type, DateTime)
        assert column.server_default is not None

    def test_column_count(self) -> None:
        assert len(CodebookCaptchaErrorCategory.__table__.columns) == 5


# ---------------------------------------------------------------------------
# CaptchaLog
# ---------------------------------------------------------------------------


class TestCaptchaLogTable:
    """Verify CaptchaLog table name, columns, and constraints."""

    def test_tablename(self) -> None:
        assert CaptchaLog.__tablename__ == "captcha_log"

    def test_id_column_is_primary_key(self) -> None:
        column = CaptchaLog.__table__.columns["id"]
        assert column.primary_key

    def test_app_id_column(self) -> None:
        column = CaptchaLog.__table__.columns["app_id"]
        assert isinstance(column.type, String)
        assert column.type.length == 64  # pyright: ignore[reportOptionalMemberAccess]
        assert column.nullable is False
        assert column.index is True
        foreign_keys = list(column.foreign_keys)
        assert len(foreign_keys) == 1
        assert foreign_keys[0].target_fullname == "apps.app"

    def test_runtime_column(self) -> None:
        column = CaptchaLog.__table__.columns["runtime"]
        assert isinstance(column.type, String)
        assert column.type.length == 64  # pyright: ignore[reportOptionalMemberAccess]
        assert column.nullable is False
        assert column.index is True
        foreign_keys = list(column.foreign_keys)
        assert len(foreign_keys) == 1
        assert foreign_keys[0].target_fullname == "runtime.runtime"

    def test_provider_name_column(self) -> None:
        column = CaptchaLog.__table__.columns["provider_name"]
        assert isinstance(column.type, String)
        assert column.type.length == 64  # pyright: ignore[reportOptionalMemberAccess]
        assert column.nullable is False

    def test_task_id_column(self) -> None:
        column = CaptchaLog.__table__.columns["task_id"]
        assert isinstance(column.type, String)
        assert column.type.length == 128  # pyright: ignore[reportOptionalMemberAccess]
        assert column.nullable is False

    def test_task_type_column(self) -> None:
        column = CaptchaLog.__table__.columns["task_type"]
        assert isinstance(column.type, String)
        assert column.type.length == 32  # pyright: ignore[reportOptionalMemberAccess]
        assert column.nullable is False

    def test_page_url_column(self) -> None:
        column = CaptchaLog.__table__.columns["page_url"]
        assert isinstance(column.type, Text)
        assert column.nullable is False

    def test_cost_column(self) -> None:
        column = CaptchaLog.__table__.columns["cost"]
        assert isinstance(column.type, Float)
        assert column.nullable is False
        assert column.server_default is not None

    def test_elapsed_seconds_column(self) -> None:
        column = CaptchaLog.__table__.columns["elapsed_seconds"]
        assert isinstance(column.type, Float)
        assert column.nullable is False

    def test_status_column_with_foreign_key(self) -> None:
        column = CaptchaLog.__table__.columns["status"]
        assert isinstance(column.type, BigInteger)
        assert column.nullable is False
        foreign_keys = list(column.foreign_keys)
        assert len(foreign_keys) == 1
        assert foreign_keys[0].target_fullname == "c_captcha_solve_status.id"

    def test_is_correct_column(self) -> None:
        column = CaptchaLog.__table__.columns["is_correct"]
        assert isinstance(column.type, Boolean)
        assert column.nullable is True

    def test_date_created_column(self) -> None:
        column = CaptchaLog.__table__.columns["date_created"]
        assert isinstance(column.type, DateTime)
        assert column.nullable is False
        assert column.server_default is not None

    def test_task_id_index_exists(self) -> None:
        index_names = [index.name for index in CaptchaLog.__table__.indexes]  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType, reportAttributeAccessIssue]
        assert "ix_captcha_log_task_id" in index_names  # pyright: ignore[reportUnknownMemberType]

    def test_column_count(self) -> None:
        assert len(CaptchaLog.__table__.columns) == 12


# ---------------------------------------------------------------------------
# CaptchaLogError
# ---------------------------------------------------------------------------


class TestCaptchaLogErrorTable:
    """Verify CaptchaLogError table name, columns, and constraints."""

    def test_tablename(self) -> None:
        assert CaptchaLogError.__tablename__ == "captcha_log_error"

    def test_id_column_is_primary_key(self) -> None:
        column = CaptchaLogError.__table__.columns["id"]
        assert column.primary_key

    def test_captcha_log_id_foreign_key(self) -> None:
        column = CaptchaLogError.__table__.columns["captcha_log_id"]
        assert isinstance(column.type, BigInteger)
        assert column.nullable is False
        assert column.unique is True
        assert column.index is True
        foreign_keys = list(column.foreign_keys)
        assert len(foreign_keys) == 1
        assert foreign_keys[0].target_fullname == "captcha_log.id"

    def test_error_code_column(self) -> None:
        column = CaptchaLogError.__table__.columns["error_code"]
        assert isinstance(column.type, String)
        assert column.type.length == 128  # pyright: ignore[reportOptionalMemberAccess]
        assert column.nullable is True

    def test_error_description_column(self) -> None:
        column = CaptchaLogError.__table__.columns["error_description"]
        assert isinstance(column.type, Text)
        assert column.nullable is True

    def test_error_category_column_with_foreign_key(self) -> None:
        column = CaptchaLogError.__table__.columns["error_category"]
        assert isinstance(column.type, BigInteger)
        assert column.nullable is True
        foreign_keys = list(column.foreign_keys)
        assert len(foreign_keys) == 1
        assert foreign_keys[0].target_fullname == "c_captcha_error_category.id"

    def test_date_created_column(self) -> None:
        column = CaptchaLogError.__table__.columns["date_created"]
        assert isinstance(column.type, DateTime)
        assert column.server_default is not None

    def test_column_count(self) -> None:
        assert len(CaptchaLogError.__table__.columns) == 6
