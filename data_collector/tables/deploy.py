"""Table deployment and seed-data utilities for framework codebooks."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, NamedTuple

import requests as http_requests
from sqlalchemy import Table

from data_collector.enums import (
    AlertSeverity,
    AppType,
    CaptchaErrorCategory,
    CaptchaSolveStatus,
    CmdFlag,
    CmdName,
    FatalFlag,
    FileRetention,
    LogLevel,
    PipelineStage,
    PipelineStatus,
    RunStatus,
    RuntimeExitCode,
)
from data_collector.settings.main import LogSettings, MainDatabaseSettings, SplunkAdminSettings
from data_collector.tables.apps import (
    CodebookAppType,
    CodebookCommandFlags,
    CodebookCommandList,
    CodebookFatalFlags,
    CodebookRunStatus,
)
from data_collector.tables.captcha import CodebookCaptchaErrorCategory, CodebookCaptchaSolveStatus
from data_collector.tables.log import CodebookLogLevel
from data_collector.tables.notifications import CodebookAlertSeverity
from data_collector.tables.pipeline import CodebookPipelineStage, CodebookPipelineStatus
from data_collector.tables.runtime import CodebookRuntimeCodes
from data_collector.tables.shared import Base
from data_collector.tables.storage import CodebookFileRetention
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import bulk_hash


@dataclass
class SeedData:
    """Seed payload + label pair."""

    data: list[Any]
    data_label: str


class _SplunkConfig(NamedTuple):
    """Resolved Splunk Management API connection parameters."""

    auth: tuple[str, str]
    base_url: str
    verify: bool
    index_name: str
    sourcetype: str


class Deploy:
    """Create/drop framework tables and seed codebooks."""

    _SPLUNK_TIMEOUT: int = 10
    _SPLUNK_DELETE_TIMEOUT: int = 30

    def __init__(self) -> None:
        self.database = Database(MainDatabaseSettings())
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def create_tables(
        self,
        tables: Sequence[Table] | None = None,
        schema: str | None = None,
    ) -> None:
        """Create tables. No args = all framework tables. With args = specific tables.

        Args:
            tables: Specific tables to create. None = all Base metadata tables.
            schema: If provided, ensure schema exists before creating tables.
        """
        if schema:
            self.database.ensure_schema(schema)
        Base.metadata.create_all(self.database.engine, tables=tables)

    def drop_tables(
        self,
        tables: Sequence[Table] | None = None,
    ) -> None:
        """Drop tables. No args = all framework tables. With args = specific tables.

        Args:
            tables: Specific tables to drop. None = all Base metadata tables.
        """
        Base.metadata.drop_all(self.database.engine, tables=tables)

    def recreate_tables(
        self,
        tables: Sequence[Table] | None = None,
        schema: str | None = None,
    ) -> None:
        """Drop then create tables. No args = all framework tables.

        Args:
            tables: Specific tables to recreate. None = all Base metadata tables.
            schema: If provided, ensure schema exists before creating tables.
        """
        self.drop_tables(tables=tables)
        self.create_tables(tables=tables, schema=schema)

    def populate_tables(self) -> bool:
        """Insert/update codebook seed rows using SHA merge flow.

        Returns:
            True if all codebooks seeded successfully, False on any error.
        """
        success = True
        with self.database.create_session() as session:
            seed_data: list[SeedData] = []

            cmd_flags = [
                CodebookCommandFlags(id=CmdFlag.PENDING.value, description="Command pending"),
                CodebookCommandFlags(id=CmdFlag.EXECUTED.value, description="Command Executed"),
                CodebookCommandFlags(
                    id=CmdFlag.NOT_EXECUTED.value,
                    description="Command not executed, conditions not meet",
                ),
            ]
            seed_data.append(SeedData(data=cmd_flags, data_label="cmd_flags"))

            cmd_list = [
                CodebookCommandList(id=CmdName.START.value, name="start", description="Start app"),
                CodebookCommandList(id=CmdName.STOP.value, name="stop", description="Stop app"),
                CodebookCommandList(id=CmdName.RESTART.value, name="restart", description="Restart app"),
                CodebookCommandList(id=CmdName.ENABLE.value, name="enable", description="Enable app"),
                CodebookCommandList(id=CmdName.DISABLE.value, name="disable", description="Disable app"),
            ]
            seed_data.append(SeedData(data=cmd_list, data_label="cmd_list"))

            fatal_flags = [
                CodebookFatalFlags(id=FatalFlag.NONE.value, description="No fatal condition"),
                CodebookFatalFlags(id=FatalFlag.FAILED_TO_START.value, description="Failed to start"),
                CodebookFatalFlags(id=FatalFlag.APP_STOPPED_ALERT_SENT.value, description="App stopped, alert sent"),
                CodebookFatalFlags(id=FatalFlag.UNEXPECTED_BEHAVIOUR.value, description="Unexpected behaviour"),
            ]
            seed_data.append(SeedData(data=fatal_flags, data_label="fatal_flags"))

            run_status = [
                CodebookRunStatus(id=RunStatus.NOT_RUNNING.value, description="App not running"),
                CodebookRunStatus(id=RunStatus.RUNNING.value, description="App is running"),
                CodebookRunStatus(
                    id=RunStatus.STOPPED.value,
                    description="App is stopped. Send command start or restart to start again.",
                ),
            ]
            seed_data.append(SeedData(data=run_status, data_label="run_status"))

            app_types = [
                CodebookAppType(id=AppType.STANDALONE.value, description="Standalone app (manual or unmanaged)"),
                CodebookAppType(id=AppType.MANAGED.value, description="Manager-scheduled app"),
                CodebookAppType(id=AppType.DRAMATIQ.value, description="Dramatiq worker actor"),
            ]
            seed_data.append(SeedData(data=app_types, data_label="app_type"))

            log_levels = [CodebookLogLevel(id=member.value, description=member.name) for member in LogLevel]
            seed_data.append(SeedData(data=log_levels, data_label="log_level"))

            runtime_codes = [
                CodebookRuntimeCodes(id=member.value, description=member.name)
                for member in RuntimeExitCode
            ]
            seed_data.append(SeedData(data=runtime_codes, data_label="runtime_codes"))

            alert_severity = [
                CodebookAlertSeverity(id=member.value, description=member.name)
                for member in AlertSeverity
            ]
            seed_data.append(SeedData(data=alert_severity, data_label="alert_severity"))

            captcha_solve_status = [
                CodebookCaptchaSolveStatus(
                    id=CaptchaSolveStatus.SOLVED.value,
                    description="Captcha solved successfully",
                ),
                CodebookCaptchaSolveStatus(
                    id=CaptchaSolveStatus.TIMED_OUT.value,
                    description="Solve attempt exceeded timeout after all retries",
                ),
                CodebookCaptchaSolveStatus(
                    id=CaptchaSolveStatus.FAILED.value,
                    description="Provider returned an API error during solve",
                ),
            ]
            seed_data.append(SeedData(data=captcha_solve_status, data_label="captcha_solve_status"))

            captcha_error_category = [
                CodebookCaptchaErrorCategory(
                    id=CaptchaErrorCategory.AUTH.value,
                    description="Bad API key or suspended account (fatal)",
                ),
                CodebookCaptchaErrorCategory(
                    id=CaptchaErrorCategory.BALANCE.value,
                    description="Zero or negative provider balance (fatal)",
                ),
                CodebookCaptchaErrorCategory(
                    id=CaptchaErrorCategory.PROXY.value,
                    description="Proxy connection or authentication error",
                ),
                CodebookCaptchaErrorCategory(
                    id=CaptchaErrorCategory.TASK.value,
                    description="Unsupported task type or bad parameters",
                ),
                CodebookCaptchaErrorCategory(
                    id=CaptchaErrorCategory.SOLVE.value,
                    description="Unsolvable captcha or worker failure",
                ),
                CodebookCaptchaErrorCategory(
                    id=CaptchaErrorCategory.RATE_LIMIT.value,
                    description="No available workers or slots",
                ),
                CodebookCaptchaErrorCategory(
                    id=CaptchaErrorCategory.UNKNOWN.value,
                    description="Unmapped or unexpected error code",
                ),
            ]
            seed_data.append(SeedData(data=captcha_error_category, data_label="captcha_error_category"))

            pipeline_status = [
                CodebookPipelineStatus(id=PipelineStatus.PENDING.value, description="Task pending"),
                CodebookPipelineStatus(id=PipelineStatus.IN_PROGRESS.value, description="Task in progress"),
                CodebookPipelineStatus(id=PipelineStatus.COMPLETED.value, description="Task completed"),
                CodebookPipelineStatus(id=PipelineStatus.FAILED.value, description="Task failed"),
                CodebookPipelineStatus(id=PipelineStatus.RETRY.value, description="Task scheduled for retry"),
            ]
            seed_data.append(SeedData(data=pipeline_status, data_label="pipeline_status"))

            pipeline_stage = [
                CodebookPipelineStage(id=PipelineStage.PREPARE.value, description="Prepare stage"),
                CodebookPipelineStage(id=PipelineStage.EXTRACT.value, description="Extract stage"),
                CodebookPipelineStage(id=PipelineStage.PROCESS.value, description="Process stage"),
                CodebookPipelineStage(id=PipelineStage.VALIDATE.value, description="Validate stage"),
                CodebookPipelineStage(id=PipelineStage.LOAD.value, description="Load stage"),
            ]
            seed_data.append(SeedData(data=pipeline_stage, data_label="pipeline_stage"))

            file_retention = [
                CodebookFileRetention(
                    id=FileRetention.TRANSIENT.value,
                    description="Temporary files, caches, intermediate processing outputs",
                    retention_days=7,
                ),
                CodebookFileRetention(
                    id=FileRetention.SHORT_TERM.value,
                    description="Operational data, session files, monitoring snapshots",
                    retention_days=90,
                ),
                CodebookFileRetention(
                    id=FileRetention.STANDARD.value,
                    description="General business records, routine correspondence",
                    retention_days=365,
                ),
                CodebookFileRetention(
                    id=FileRetention.REGULATORY_3Y.value,
                    description="Employment records, customer complaints, warranty documentation",
                    retention_days=1095,
                ),
                CodebookFileRetention(
                    id=FileRetention.REGULATORY_5Y.value,
                    description="Accounting records, financial statements, contractual documents",
                    retention_days=1825,
                ),
                CodebookFileRetention(
                    id=FileRetention.REGULATORY_7Y.value,
                    description="Tax and audit records, securities compliance, AML records",
                    retention_days=2555,
                ),
                CodebookFileRetention(
                    id=FileRetention.REGULATORY_10Y.value,
                    description="Banking records, insurance documents, healthcare records",
                    retention_days=3650,
                ),
                CodebookFileRetention(
                    id=FileRetention.EXTENDED.value,
                    description="Legal holds, product liability, engineering records",
                    retention_days=9125,
                ),
                CodebookFileRetention(
                    id=FileRetention.PERMANENT.value,
                    description="Permanent storage, never deleted by retention enforcement",
                    retention_days=None,
                ),
            ]
            seed_data.append(SeedData(data=file_retention, data_label="file_retention"))

            for seed in seed_data:
                try:
                    bulk_hash(seed.data)
                    self.database.merge(seed.data, session)
                except Exception as exc:
                    success = False
                    self.logger.error("Failed to populate %s: %s", seed.data_label, exc)

        return success

    def _get_splunk_config(self) -> _SplunkConfig | None:
        """Resolve Splunk Management API settings; return None when credentials are missing."""
        log_settings = LogSettings()
        admin = SplunkAdminSettings()

        if not admin.admin_user or not admin.admin_password:
            self.logger.error("DC_SPLUNK_ADMIN_USER and DC_SPLUNK_ADMIN_PASSWORD must be set.")
            return None

        return _SplunkConfig(
            auth=(admin.admin_user, admin.admin_password),
            base_url=admin.mgmt_url.rstrip("/"),
            verify=admin.verify_tls,
            index_name=log_settings.splunk_index,
            sourcetype=log_settings.splunk_sourcetype,
        )

    def setup_splunk(self) -> bool:
        """Create Splunk index and sourcetype via Management API.

        Reads index name and sourcetype from LogSettings, admin credentials
        from SplunkAdminSettings. Skips creation if the index already exists.

        Returns:
            True if provisioning succeeded or was already done, False on error.
        """
        cfg = self._get_splunk_config()
        if cfg is None:
            return False

        if not self._splunk_ensure_index(cfg.base_url, cfg.auth, cfg.verify, cfg.index_name):
            return False

        return self._splunk_ensure_sourcetype(cfg.base_url, cfg.auth, cfg.verify, cfg.sourcetype)

    def clean_splunk(self) -> bool:
        """Empty all data from the Splunk index while keeping its definition.

        Returns:
            True if the clean succeeded, False on error.
        """
        cfg = self._get_splunk_config()
        if cfg is None:
            return False

        delete_url = f"{cfg.base_url}/services/data/indexes/{cfg.index_name}"
        try:
            resp = http_requests.delete(
                delete_url, auth=cfg.auth, verify=cfg.verify,
                params={"output_mode": "json"}, timeout=self._SPLUNK_DELETE_TIMEOUT,
            )
            if not self._splunk_check_auth(resp, f"delete index '{cfg.index_name}'", "indexes_edit"):
                return False
            resp.raise_for_status()
            self.logger.info("Splunk index '%s' deleted.", cfg.index_name)
        except http_requests.RequestException as exc:
            self.logger.error("Failed to delete Splunk index '%s': %s", cfg.index_name, exc)
            return False

        if not self._splunk_ensure_index(cfg.base_url, cfg.auth, cfg.verify, cfg.index_name):
            return False

        self.logger.info("Splunk index '%s' recreated (data cleared).", cfg.index_name)
        return True

    def _splunk_check_auth(self, resp: http_requests.Response, action: str, capability: str) -> bool:
        """Return False and log when Splunk returns 401/403; True otherwise."""
        if resp.status_code == 401:
            self.logger.error("Splunk authentication failed (%s). Check DC_SPLUNK_ADMIN_USER/PASSWORD.", action)
            return False
        if resp.status_code == 403:
            self.logger.error(
                "Splunk authorization failed (%s). "
                "Ask your Splunk admin to grant the '%s' capability to your role.",
                action,
                capability,
            )
            return False
        return True

    def _splunk_ensure_index(
        self,
        base: str,
        auth: tuple[str, str],
        verify: bool,
        index_name: str,
    ) -> bool:
        """Check if a Splunk index exists; create it if missing."""
        url = f"{base}/services/data/indexes/{index_name}"
        try:
            resp = http_requests.get(
                url, auth=auth, verify=verify,
                params={"output_mode": "json"}, timeout=self._SPLUNK_TIMEOUT,
            )
            if not self._splunk_check_auth(resp, f"check index '{index_name}'", "indexes_edit"):
                return False
            if resp.status_code == 200:
                self.logger.info("Splunk index '%s' already exists.", index_name)
                return True
        except http_requests.RequestException as exc:
            self.logger.error("Failed to check Splunk index '%s': %s", index_name, exc)
            return False

        create_url = f"{base}/services/data/indexes"
        try:
            resp = http_requests.post(
                create_url,
                auth=auth,
                verify=verify,
                data={"name": index_name, "output_mode": "json"},
                timeout=self._SPLUNK_TIMEOUT,
            )
            if not self._splunk_check_auth(resp, f"create index '{index_name}'", "indexes_edit"):
                return False
            resp.raise_for_status()
            self.logger.info("Splunk index '%s' created.", index_name)
            return True
        except http_requests.RequestException as exc:
            self.logger.error("Failed to create Splunk index '%s': %s", index_name, exc)
            return False

    def _splunk_ensure_sourcetype(
        self,
        base: str,
        auth: tuple[str, str],
        verify: bool,
        sourcetype: str,
    ) -> bool:
        """Check if a Splunk sourcetype exists; create it if missing."""
        url = f"{base}/servicesNS/nobody/search/configs/conf-props/{sourcetype}"
        try:
            resp = http_requests.get(
                url, auth=auth, verify=verify,
                params={"output_mode": "json"}, timeout=self._SPLUNK_TIMEOUT,
            )
            if not self._splunk_check_auth(resp, f"check sourcetype '{sourcetype}'", "admin_all_objects"):
                return False
            if resp.status_code == 200:
                self.logger.info("Splunk sourcetype '%s' already exists.", sourcetype)
                return True
        except http_requests.RequestException as exc:
            self.logger.error("Failed to check Splunk sourcetype '%s': %s", sourcetype, exc)
            return False

        create_url = f"{base}/servicesNS/nobody/search/configs/conf-props"
        try:
            resp = http_requests.post(
                create_url,
                auth=auth,
                verify=verify,
                data={"name": sourcetype, "output_mode": "json"},
                timeout=self._SPLUNK_TIMEOUT,
            )
            if not self._splunk_check_auth(resp, f"create sourcetype '{sourcetype}'", "admin_all_objects"):
                return False
            resp.raise_for_status()
            self.logger.info("Splunk sourcetype '%s' created.", sourcetype)
            return True
        except http_requests.RequestException as exc:
            self.logger.error("Failed to create Splunk sourcetype '%s': %s", sourcetype, exc)
            return False


EXAMPLE_SCHEMA = "dc_example"


class ExampleDeploy(Deploy):
    """Deploy variant that isolates all tables into the ``dc_example`` schema.

    Uses SQLAlchemy ``schema_translate_map`` to redirect framework tables (schema=None)
    and example data tables (schema="scraping") into ``dc_example`` without modifying ORM
    model definitions. Production ``Deploy`` remains untouched.
    """

    def __init__(self) -> None:
        self.database = Database(
            MainDatabaseSettings(),
            schema_translate_map={None: EXAMPLE_SCHEMA, "scraping": EXAMPLE_SCHEMA},
        )
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def create_tables(
        self,
        tables: Sequence[Table] | None = None,
        schema: str | None = None,
    ) -> None:
        """Create tables in the dc_example schema.

        Args:
            tables: Specific tables to create. None = all Base metadata tables.
            schema: Ignored. The dc_example schema is always used.
        """
        self.database.ensure_schema(EXAMPLE_SCHEMA)
        Base.metadata.create_all(self.database.engine, tables=tables)

    def drop_tables(
        self,
        tables: Sequence[Table] | None = None,
    ) -> None:
        """Drop tables from the dc_example schema.

        Args:
            tables: Specific tables to drop. None = all Base metadata tables.
        """
        Base.metadata.drop_all(self.database.engine, tables=tables)
