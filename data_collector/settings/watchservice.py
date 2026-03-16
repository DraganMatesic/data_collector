"""Pydantic settings for WatchService file system monitoring."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class WatchServiceSettings(BaseSettings):
    """WatchService settings loaded from environment variables.

    Controls observer type, stability debounce timing, reconciliation
    intervals, and writer batch sizing for the file system watcher.

    Attributes:
        observer: ``DC_WATCHER_OBSERVER`` -- Observer type: ``auto`` (native OS API) or ``polling`` (fallback).
        poll_interval: ``DC_WATCHER_POLL_INTERVAL`` -- Polling interval in seconds (PollingObserver only).
        reconcile_interval: ``DC_WATCHER_RECONCILE_INTERVAL`` -- Seconds between reconciliation directory scans.
        debounce: ``DC_WATCHER_DEBOUNCE`` -- Seconds for file stability debounce (size-check-twice delay).
        stability_timeout: ``DC_WATCHER_STABILITY_TIMEOUT`` -- Max seconds to wait for an unstable file.
        stabilization_grace: ``DC_WATCHER_STABILIZATION_GRACE`` -- Grace seconds after stabilization to ignore MODIFIED.
        watched_dirs_root: ``DC_WATCHER_WATCHED_DIRS_ROOT`` -- Base directory containing watched subdirectories.
        writer_batch_size: ``DC_WATCHER_WRITER_BATCH_SIZE`` -- Maximum events per writer thread batch.
        reconcile_batch_size: ``DC_WATCHER_RECONCILE_BATCH_SIZE`` -- Max path hashes per IN clause in reconciliation.

    Examples:
        From environment variables::

            settings = WatchServiceSettings()

        Direct construction (testing, overrides)::

            settings = WatchServiceSettings(observer="polling", debounce=5.0)
    """

    model_config = SettingsConfigDict(env_prefix="DC_WATCHER_")

    observer: str = "auto"
    poll_interval: int = 5
    reconcile_interval: int = 60
    debounce: float = 2.0
    stability_timeout: int = 60
    stabilization_grace: int = 5
    watched_dirs_root: str = "./watched"
    writer_batch_size: int = 50
    reconcile_batch_size: int = 500
