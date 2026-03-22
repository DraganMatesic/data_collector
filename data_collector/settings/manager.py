"""Pydantic settings for the orchestration manager."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class ManagerSettings(BaseSettings):
    """Orchestration manager settings loaded from environment variables.

    Environment variables follow the ``DC_MANAGER_`` prefix pattern:

        DC_MANAGER_POLLING_INTERVAL     -- Seconds between main loop ticks.
        DC_MANAGER_COMMAND_POLL_INTERVAL -- Seconds between DB command polls.
        DC_MANAGER_PROCESS_CHECK_INTERVAL -- Seconds between PID alive checks.
        DC_MANAGER_STARTUP_GRACE_PERIOD -- Seconds before first PID check after spawn.
        DC_MANAGER_MAX_START_FAILURES   -- Consecutive spawn failures before FAILED_TO_START.
        DC_MANAGER_RABBITMQ_ENABLED     -- Enable RabbitMQ command consumer.
        DC_MANAGER_NOTIFICATIONS_ENABLED -- Enable notification dispatch.
        DC_MANAGER_SHUTDOWN_TIMEOUT     -- Max seconds for child process shutdown.
        DC_MANAGER_RETENTION_ENABLED    -- Enable periodic retention cleanup.
        DC_MANAGER_RETENTION_CHECK_INTERVAL -- Seconds between retention runs.
        DC_MANAGER_RETENTION_LOG_DAYS   -- Days to keep log records.
        DC_MANAGER_RETENTION_RUNTIME_DAYS -- Days to keep runtime records.
        DC_MANAGER_RETENTION_FUNCTION_LOG_DAYS -- Days to keep function_log records.
        DC_MANAGER_RETENTION_COMMAND_LOG_DAYS -- Days to keep command_log records.
        DC_MANAGER_RETENTION_APP_PURGE_ENABLED -- Enable automatic purge of removed apps.
        DC_MANAGER_STORAGE_JANITOR_ENABLED     -- Enable periodic storage maintenance.
        DC_MANAGER_STORAGE_JANITOR_CHECK_INTERVAL -- Seconds between storage maintenance runs.

    Examples:
        From environment variables::

            settings = ManagerSettings()
            manager = Manager(database, settings, logger=logger)

        Direct construction (testing, overrides)::

            settings = ManagerSettings(
                polling_interval=5,
                rabbitmq_enabled=True,
                notifications_enabled=True,
            )
    """

    model_config = SettingsConfigDict(env_prefix="DC_MANAGER_")

    # -- Main loop --
    polling_interval: int = 10
    command_poll_interval: int = 5
    process_check_interval: int = 30
    startup_grace_period: int = 10
    max_start_failures: int = 3

    # -- Optional integrations --
    rabbitmq_enabled: bool = True
    notifications_enabled: bool = False

    # -- Shutdown --
    shutdown_timeout: int = 30

    # -- Retention --
    retention_enabled: bool = True
    retention_check_interval: int = 3600
    retention_log_days: int = 90
    retention_runtime_days: int = 180
    retention_function_log_days: int = 90
    retention_command_log_days: int = 365
    retention_app_purge_enabled: bool = True

    # -- Storage janitor --
    storage_janitor_enabled: bool = True
    storage_janitor_check_interval: int = 3600
