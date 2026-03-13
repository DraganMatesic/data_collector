"""Solve reCAPTCHA v2 on Google's demo page with full database persistence.

Demonstrates:
    - Full app lifecycle: Deploy, app registration, Runtime, LoggingService
    - AntiCaptchaProvider with database persistence (CaptchaLog rows)
    - @fun_watch decorator for function-level logging and metrics
    - CaptchaErrorCategory for retry/abort decisions
    - report_correct() / report_incorrect() for solver feedback
    - Querying CaptchaLog to verify persistence

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT, DC_CAPTCHA_API_KEY environment variables.

Run:
    python -m data_collector.examples run captcha/solve_recaptcha/main
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from data_collector.captcha import AntiCaptchaProvider, CaptchaMetrics
from data_collector.captcha.models import CaptchaError, CaptchaResult, CaptchaTimeout
from data_collector.enums import FatalFlag, RunStatus
from data_collector.enums.captcha import CaptchaErrorCategory
from data_collector.scraping.base import update_app_status
from data_collector.settings.captcha import CaptchaSettings
from data_collector.settings.main import LogSettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.captcha import CaptchaLog, CaptchaLogError
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

_DEMO_URL = "https://www.google.com/recaptcha/api2/demo"
_SITEKEY_PATTERN = re.compile(r'data-sitekey="([^"]+)"')


class RecaptchaSolver(FunWatchMixin):
    """Solve reCAPTCHA v2 on a demo page with full framework integration."""

    def __init__(
        self,
        provider: AntiCaptchaProvider,
        request: Request,
        database: Database,
        *,
        app_id: str,
        runtime: str,
        logger: Any,
    ) -> None:
        self.provider = provider
        self.request = request
        self.database = database
        self.app_id = app_id
        self.runtime = runtime
        self.logger = logger

    @fun_watch
    def fetch_demo_page(self) -> tuple[str, object]:
        """Fetch the reCAPTCHA v2 demo page and extract the sitekey."""
        self.logger.info("Fetching demo page", extra={"url": _DEMO_URL})
        response = self.request.get(_DEMO_URL)
        if response is None:
            raise RuntimeError(f"Failed to fetch demo page: {_DEMO_URL}")

        html = response.text
        match = _SITEKEY_PATTERN.search(html)
        if match is None:
            raise RuntimeError("Could not extract data-sitekey from demo page HTML")

        site_key = match.group(1)
        self.logger.info("Extracted sitekey", extra={"site_key": site_key})
        return site_key, response.cookies

    @fun_watch
    def solve_captcha(self, site_key: str) -> CaptchaResult:
        """Solve reCAPTCHA v2 via AntiCaptcha. CaptchaLog row created automatically."""
        self.logger.info("Solving reCAPTCHA v2", extra={"site_key": site_key, "page_url": _DEMO_URL})
        result = self.provider.solve_recaptcha_v2(site_key=site_key, page_url=_DEMO_URL)
        self.logger.info(
            "Captcha solved",
            extra={"task_id": result.task_id, "cost": result.cost, "elapsed": result.elapsed_seconds},
        )
        return result

    @fun_watch
    def submit_and_report(self, result: CaptchaResult, cookies: object) -> bool:
        """Submit solved token to demo form and report feedback to provider."""
        self.logger.info("Submitting solved token to demo form")
        submit_response = self.request.post(
            _DEMO_URL,
            data={"g-recaptcha-response": result.solution},
            cookies=cookies,
        )

        success = False
        if submit_response is not None:
            success = "Verification Success" in submit_response.text
            self.logger.info("Form submission result", extra={"success": success})
        else:
            self.logger.warning("Form submission failed: no response")

        if success:
            reported = self.provider.report_correct(result.task_id, result.task_type)
            self.logger.info("Reported correct", extra={"accepted": reported})
        else:
            reported = self.provider.report_incorrect(result.task_id, result.task_type)
            self.logger.info("Reported incorrect", extra={"accepted": reported})

        return success

    @fun_watch
    def display_captcha_log(self) -> list[CaptchaLog]:
        """Query and display CaptchaLog rows created during this runtime."""
        statement = select(CaptchaLog).where(CaptchaLog.runtime == self.runtime)
        with self.database.create_session() as session:
            rows = list(session.execute(statement).scalars().all())

            print(f"\n=== CaptchaLog rows for this runtime ({len(rows)} total) ===")
            for row in rows:
                print(f"  task_id:      {row.task_id}")
                print(f"  task_type:    {row.task_type}")
                print(f"  status:       {row.status}")
                print(f"  cost:         ${row.cost:.5f}")
                print(f"  elapsed:      {row.elapsed_seconds}s")
                print(f"  is_correct:   {row.is_correct}")
                print(f"  provider:     {row.provider_name}")
                print(f"  page_url:     {row.page_url}")

                error_row = session.execute(
                    select(CaptchaLogError).where(CaptchaLogError.captcha_log_id == row.id)
                ).scalar_one_or_none()
                if error_row is not None:
                    print(f"  error_code:   {error_row.error_code}")
                    print(f"  error_desc:   {error_row.error_description}")
                    print(f"  error_cat:    {error_row.error_category}")

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
                managed=False,
            ),
            session,
            filter_cols=["group_name", "parent_name", "app_name"],
        )


def _update_runtime(
    database: Database, runtime: str, start_time: datetime, solved: int, failed: int,
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
    """End-to-end reCAPTCHA v2 solve with full database persistence."""
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
    metrics = CaptchaMetrics()
    request = Request(timeout=30, retries=2)

    provider = AntiCaptchaProvider(
        api_key=captcha_settings.api_key,
        request=request,
        timeout=captcha_settings.timeout,
        max_retries=captcha_settings.max_retries,
        poll_interval=captcha_settings.poll_interval,
        metrics=metrics,
        database=database,
        app_id=app_id,
        runtime=runtime,
    )

    solver = RecaptchaSolver(
        provider=provider,
        request=request,
        database=database,
        app_id=app_id,
        runtime=runtime,
        logger=logger,
    )

    solved = 0
    failed = 0
    try:
        site_key, cookies = solver.fetch_demo_page()
        result = solver.solve_captcha(site_key)
        success = solver.submit_and_report(result, cookies)

        if success:
            solved = 1
        else:
            failed = 1

        solver.display_captcha_log()

        metrics.log_stats(logging.getLogger(__name__))

        print(f"\nSolve recaptcha completed: solved={solved}, failed={failed}")

        update_app_status(
            database, app_id,
            run_status=RunStatus.NOT_RUNNING,
            solved=solved,
            failed=failed,
            task_size=1,
            fatal_flag=FatalFlag.NONE,
        )

    except CaptchaTimeout as error:
        failed = 1
        logger.error("Captcha timed out", extra={"task_id": error.task_id, "timeout": error.timeout_seconds})
        solver.display_captcha_log()
        update_app_status(
            database, app_id,
            run_status=RunStatus.NOT_RUNNING,
            solved=solved,
            failed=failed,
            task_size=1,
            fatal_flag=FatalFlag.UNEXPECTED_BEHAVIOUR,
            fatal_msg=str(error),
            fatal_time=datetime.now(UTC),
        )

    except CaptchaError as error:
        failed = 1
        logger.error(
            "Captcha API error",
            extra={"error_code": error.error_code, "category": error.category.name},
        )
        if error.category in (CaptchaErrorCategory.AUTH, CaptchaErrorCategory.BALANCE):
            logger.error("Fatal captcha error -- check API key or balance")
        solver.display_captcha_log()
        update_app_status(
            database, app_id,
            run_status=RunStatus.NOT_RUNNING,
            solved=solved,
            failed=failed,
            task_size=1,
            fatal_flag=FatalFlag.FAILED_TO_START,
            fatal_msg=f"[{error.error_code}] {error.error_description}",
            fatal_time=datetime.now(UTC),
        )

    finally:
        _update_runtime(database, runtime, start_time, solved, failed)
        FunWatchRegistry.reset()
        service.stop()
