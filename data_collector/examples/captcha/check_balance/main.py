"""Check AntiCaptcha account balance with full app lifecycle.

Demonstrates:
    - Full app lifecycle: Deploy, app registration, Runtime, LoggingService
    - AntiCaptchaProvider.get_balance() API call
    - CaptchaErrorCategory for error classification and retry/abort decisions
    - @fun_watch decorator for function-level logging and metrics
    - CaptchaLog query (shows 0 rows -- get_balance is a utility call, not a solve)

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT, DC_CAPTCHA_API_KEY environment variables.

Run:
    python -m data_collector.examples run captcha/check_balance/main
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from data_collector.captcha import AntiCaptchaProvider
from data_collector.captcha.models import CaptchaError
from data_collector.enums import FatalFlag, RunStatus
from data_collector.enums.captcha import CaptchaErrorCategory
from data_collector.scraping.base import update_app_status
from data_collector.settings.captcha import CaptchaSettings
from data_collector.settings.main import LogSettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.captcha import CaptchaLog
from data_collector.tables.deploy import Deploy
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchMixin, FunWatchRegistry, fun_watch
from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals
from data_collector.utilities.functions.runtime import AppInfo, get_app_info
from data_collector.utilities.log.main import LoggingService
from data_collector.utilities.request import Request

_REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT", "DC_CAPTCHA_API_KEY",
)


class BalanceChecker(FunWatchMixin):
    """Check captcha provider balance with full framework integration."""

    def __init__(
        self,
        provider: AntiCaptchaProvider,
        database: Database,
        *,
        app_id: str,
        runtime: str,
        logger: Any,
    ) -> None:
        self.provider = provider
        self.database = database
        self.app_id = app_id
        self.runtime = runtime
        self.logger = logger

    @fun_watch
    def check_balance(self) -> float:
        """Query the AntiCaptcha account balance."""
        self.logger.info("Querying account balance")
        try:
            balance = self.provider.get_balance()
        except CaptchaError as error:
            self.logger.error(
                "Balance check failed",
                extra={"error_code": error.error_code, "category": error.category.name},
            )
            if error.category == CaptchaErrorCategory.AUTH:
                self.logger.error("Auth error -- check that DC_CAPTCHA_API_KEY is valid and the account is active")
            elif error.category == CaptchaErrorCategory.BALANCE:
                self.logger.error("Balance error -- account may be depleted")
            raise

        self.logger.info("Balance retrieved", extra={"balance_usd": balance})
        print(f"\n  Account balance: ${balance:.4f} USD")
        return balance

    @fun_watch
    def display_captcha_log(self) -> list[CaptchaLog]:
        """Query and display CaptchaLog rows created during this runtime.

        get_balance() is a utility call -- it does not create CaptchaLog rows.
        CaptchaLog records are created only by solve methods (solve_recaptcha_v2,
        solve_image, etc.). This method demonstrates the query pattern and
        confirms that 0 rows were created, which is correct behavior.
        """
        statement = select(CaptchaLog).where(CaptchaLog.runtime == self.runtime)
        with self.database.create_session() as session:
            rows = list(session.execute(statement).scalars().all())

        print(f"\n=== CaptchaLog rows for this runtime ({len(rows)} total) ===")
        if not rows:
            print("  (none) -- get_balance() does not create CaptchaLog rows.")
            print("  CaptchaLog records are created only by solve methods.")
        for row in rows:
            print(f"  task_id:      {row.task_id}")
            print(f"  task_type:    {row.task_type}")
            print(f"  status:       {row.status}")
            print(f"  date_created: {row.date_created}")
            print()

        return rows


def _register_app(database: Database, app_info: AppInfo) -> None:
    """Register AppGroups, AppParents, and Apps rows using idempotent update_insert."""
    with database.create_session() as session:
        database.update_insert(AppGroups(name=app_info["app_group"]), session, filter_cols=["name"])

    with database.create_session() as session:
        database.update_insert(
            AppParents(name=app_info["app_parent"], group_name=app_info["app_group"], parent=app_info["parent_id"]),
            session,
            filter_cols=["name", "group_name"],
        )

    with database.create_session() as session:
        database.update_insert(
            Apps(
                app=app_info["app_id"],
                group_name=app_info["app_group"],
                parent_name=app_info["app_parent"],
                app_name=app_info["app_name"],
                parent_id=app_info["parent_id"],
                run_status=RunStatus.NOT_RUNNING,
                fatal_flag=FatalFlag.NONE,
                disable=True,
            ),
            session,
            filter_cols=["group_name", "parent_name", "app_name"],
        )


def _update_runtime(
    database: Database, runtime: str, start_time: datetime, failed: int,
) -> None:
    """Finalize the Runtime record with end time and counters."""
    end_time = datetime.now(UTC)
    with database.create_session() as session:
        runtime_record = session.execute(
            select(Runtime).where(Runtime.runtime == runtime)
        ).scalar_one_or_none()
        if runtime_record is not None:
            session.merge(Runtime(
                id=runtime_record.id,
                runtime=runtime,
                app_id=runtime_record.app_id,
                start_time=runtime_record.start_time,
                end_time=end_time,
                task_size=1,
                except_cnt=failed,
                totals=get_totals(start_time, end_time),
                totalm=get_totalm(start_time, end_time),
                totalh=get_totalh(start_time, end_time),
            ))
            session.commit()


def main() -> None:
    """End-to-end balance check with full app lifecycle."""
    missing = [variable for variable in _REQUIRED_ENV if not os.environ.get(variable)]
    if missing:
        print(f"Skipping: env vars not set: {', '.join(missing)}")
        return

    FunWatchRegistry.reset()

    deploy = Deploy()
    deploy.create_tables()
    deploy.populate_tables()

    database = deploy.database
    app_info: AppInfo = get_app_info(__file__)  # type: ignore[assignment]
    app_id = app_info["app_id"]
    app_group = app_info["app_group"]
    app_parent = app_info["app_parent"]
    app_name = app_info["app_name"]

    _register_app(database, app_info)
    database.app_id = app_id

    log_settings = LogSettings(log_level=10, log_error_file="error.log")
    FunWatchRegistry.instance().set_default_lifecycle_log_level(log_settings.log_level)
    service = LoggingService(
        f"{app_group}.{app_parent}.{app_name}", settings=log_settings, db_engine=database.engine,
    )
    logger = service.configure_logger()

    runtime = uuid.uuid4().hex
    logger = logger.bind(app_id=app_id, runtime=runtime)

    update_app_status(database, app_id, run_status=RunStatus.RUNNING, runtime_id=runtime)

    start_time = datetime.now(UTC)
    with database.create_session() as session:
        session.merge(Runtime(runtime=runtime, app_id=app_id, start_time=start_time))
        session.commit()

    captcha_settings = CaptchaSettings()  # type: ignore[call-arg]
    request = Request(timeout=30)

    provider = AntiCaptchaProvider(
        api_key=captcha_settings.api_key,
        request=request,
        database=database,
        app_id=app_id,
        runtime=runtime,
    )

    checker = BalanceChecker(
        provider=provider,
        database=database,
        app_id=app_id,
        runtime=runtime,
        logger=logger,
    )

    failed = 0
    try:
        checker.check_balance()
        checker.display_captcha_log()

        print("\nCheck balance completed successfully")

        update_app_status(
            database, app_id,
            run_status=RunStatus.NOT_RUNNING,
            solved=1,
            failed=0,
            task_size=1,
            fatal_flag=FatalFlag.NONE,
        )

    except CaptchaError as error:
        failed = 1
        checker.display_captcha_log()
        fatal_flag = FatalFlag.FAILED_TO_START if error.category in (
            CaptchaErrorCategory.AUTH, CaptchaErrorCategory.BALANCE,
        ) else FatalFlag.UNEXPECTED_BEHAVIOUR
        update_app_status(
            database, app_id,
            run_status=RunStatus.NOT_RUNNING,
            solved=0,
            failed=1,
            task_size=1,
            fatal_flag=fatal_flag,
            fatal_msg=f"[{error.error_code}] {error.error_description}",
            fatal_time=datetime.now(UTC),
        )

    finally:
        _update_runtime(database, runtime, start_time, failed)
        FunWatchRegistry.reset()
        service.stop()
