"""Pydantic settings for Dramatiq workers and TaskDispatcher configuration."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class DramatiqSettings(BaseSettings):
    """Dramatiq worker settings loaded from environment variables.

    Controls worker thread/process counts, retry behaviour, time/age
    limits, and service log configuration for Dramatiq task processing.

    Attributes:
        workers: ``DC_DRAMATIQ_WORKERS`` -- Number of worker threads per process.
        processes: ``DC_DRAMATIQ_PROCESSES`` -- Number of worker processes.
        max_retries: ``DC_DRAMATIQ_MAX_RETRIES`` -- Maximum retry attempts per task before dead-lettering.
        min_backoff: ``DC_DRAMATIQ_MIN_BACKOFF`` -- Minimum retry backoff in milliseconds.
        max_backoff: ``DC_DRAMATIQ_MAX_BACKOFF`` -- Maximum retry backoff in milliseconds (5 minutes).
        time_limit: ``DC_DRAMATIQ_TIME_LIMIT`` -- Maximum actor execution time in milliseconds (10 minutes).
        max_age: ``DC_DRAMATIQ_MAX_AGE`` -- Maximum message age in milliseconds before discarding (24 hours).
        queues: ``DC_DRAMATIQ_QUEUES`` -- Comma-separated queue names to consume. Empty string means all queues.
        log_file: ``DC_DRAMATIQ_LOG_FILE`` -- Path to the service log file. Relative paths resolve from project root.
        log_max_bytes: ``DC_DRAMATIQ_LOG_MAX_BYTES`` -- Maximum log file size in bytes before rotation (10 MB).
        log_backup_count: ``DC_DRAMATIQ_LOG_BACKUP_COUNT`` -- Number of rotated log files to keep.

    Examples:
        From environment variables::

            settings = DramatiqSettings()

        Direct construction (testing, overrides)::

            settings = DramatiqSettings(workers=8, processes=2)
    """

    model_config = SettingsConfigDict(env_prefix="DC_DRAMATIQ_")

    workers: int = 4
    processes: int = 1
    max_retries: int = 3
    min_backoff: int = 1000
    max_backoff: int = 300_000
    time_limit: int = 600_000
    max_age: int = 86_400_000
    queues: str = ""
    log_file: str = "dramatiq_service.log"
    log_max_bytes: int = 10_485_760
    log_backup_count: int = 5


class TaskDispatcherSettings(BaseSettings):
    """TaskDispatcher settings loaded from environment variables.

    Controls how frequently the TaskDispatcher polls the Events table
    and how many events are processed per batch.

    Attributes:
        batch_size: ``DC_DISPATCHER_BATCH_SIZE`` -- Maximum number of events to process per dispatch batch.
        poll_interval: ``DC_DISPATCHER_POLL_INTERVAL`` -- Seconds to wait between polls when queue is empty.

    Examples:
        From environment variables::

            settings = TaskDispatcherSettings()

        Direct construction (testing, overrides)::

            settings = TaskDispatcherSettings(batch_size=50, poll_interval=5)
    """

    model_config = SettingsConfigDict(env_prefix="DC_DISPATCHER_")

    batch_size: int = 100
    poll_interval: int = 10
