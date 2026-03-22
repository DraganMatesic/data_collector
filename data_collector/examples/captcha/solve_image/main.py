"""Solve an image captcha from AntiCaptcha's demo form with full database persistence.

Demonstrates:
    - Full app lifecycle: Deploy, app registration, Runtime, LoggingService
    - AntiCaptchaProvider.solve_image() with database persistence (CaptchaLog rows)
    - @fun_watch decorator for function-level logging and metrics
    - CaptchaErrorCategory for error classification
    - Extracting captcha image URL from HTML
    - Querying CaptchaLog to verify persistence

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT, DC_CAPTCHA_API_KEY environment variables.

Run:
    python -m data_collector.examples run captcha/solve_image/main
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
from data_collector.tables.deploy import ExampleDeploy
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

_DEMO_URL = "https://democaptcha.com/demo-form-eng/image.html"
_IMAGE_PATTERN = re.compile(r'<img[^>]+src="([^"]+)"[^>]*id="captcha-image"', re.IGNORECASE)
_IMAGE_PATTERN_ALT = re.compile(r'<img[^>]+src="(https?://[^"]*captcha[^"]*)"', re.IGNORECASE)


def _extract_image_url(html: str, base_url: str) -> str | None:
    """Extract the captcha image URL from the demo page HTML."""
    match = _IMAGE_PATTERN.search(html)
    if match is None:
        match = _IMAGE_PATTERN_ALT.search(html)
    if match is None:
        return None
    image_url = match.group(1)
    if image_url.startswith("/"):
        domain_match = re.match(r"(https?://[^/]+)", base_url)
        if domain_match:
            image_url = domain_match.group(1) + image_url
    return image_url


class ImageSolver(FunWatchMixin):
    """Solve image captcha from a demo form with full framework integration."""

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
    def fetch_demo_page(self) -> str:
        """Fetch the image captcha demo page and extract the image URL."""
        self.logger.info("Fetching demo page", extra={"url": _DEMO_URL})
        response = self.request.get(_DEMO_URL)
        if response is None:
            raise RuntimeError(f"Failed to fetch demo page: {_DEMO_URL}")

        image_url = _extract_image_url(response.text, _DEMO_URL)
        if image_url is None:
            raise RuntimeError("Could not extract captcha image URL from demo page HTML")

        self.logger.info("Extracted image URL", extra={"image_url": image_url})
        return image_url

    @fun_watch
    def download_image(self, image_url: str) -> bytes:
        """Download the captcha image bytes."""
        self.logger.info("Downloading captcha image", extra={"image_url": image_url})
        image_response = self.request.get(image_url)
        if image_response is None:
            raise RuntimeError(f"Failed to download captcha image: {image_url}")

        image_bytes = image_response.content
        self.logger.info("Image downloaded", extra={"size_bytes": len(image_bytes)})
        return image_bytes

    @fun_watch
    def solve_captcha(self, image_bytes: bytes) -> CaptchaResult:
        """Solve image captcha via AntiCaptcha. CaptchaLog row created automatically."""
        self.logger.info("Solving image captcha", extra={"image_size": len(image_bytes)})
        result = self.provider.solve_image(image_bytes, page_url=_DEMO_URL)
        self.logger.info(
            "Image captcha solved",
            extra={"task_id": result.task_id, "solution": result.solution, "cost": result.cost},
        )
        return result

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
    """End-to-end image captcha solve with full database persistence."""
    missing = [variable for variable in _REQUIRED_ENV if not os.environ.get(variable)]
    if missing:
        print(f"Skipping: env vars not set: {', '.join(missing)}")
        return

    FunWatchRegistry.reset()

    deploy = ExampleDeploy()
    deploy.create_tables()
    deploy.populate_tables()
    FunWatchRegistry.instance().set_system_db(deploy.database)

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

    solver = ImageSolver(
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
        image_url = solver.fetch_demo_page()
        image_bytes = solver.download_image(image_url)
        result = solver.solve_captcha(image_bytes)

        print(f"\nImage captcha recognized: {result.solution}")
        solved = 1

        solver.display_captcha_log()

        metrics.log_stats(logging.getLogger(__name__))

        print(f"\nSolve image completed: solved={solved}, failed={failed}")

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
