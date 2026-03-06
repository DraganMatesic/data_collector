"""Real Splunk HEC logging with framework LoggingService.

Demonstrates:
    - Loading Splunk HEC settings from `DC_LOG_SPLUNK_*` environment variables
    - Configuring `LoggingService` with Splunk sink enabled and DB sink disabled
    - Binding `app_id` + `runtime` once and reusing bound context
    - Emitting structured events with arbitrary context fields
    - Fallback behavior when Splunk HEC is unreachable

Expected environment:
    DC_LOG_SPLUNK_ENABLED=true
    DC_LOG_SPLUNK_URL=https://127.0.0.1:8088/services/collector
    DC_LOG_SPLUNK_TOKEN=<your-hec-token>
    DC_LOG_SPLUNK_VERIFY_TLS=false
    # Optional:
    # DC_LOG_SPLUNK_CA_BUNDLE=<path-to-ca-bundle>
    # DC_LOG_SPLUNK_INDEX=main                    (default: "default")
    # DC_LOG_SPLUNK_SOURCETYPE=myapp:json          (default: "data_collector:structured")
    # DC_LOG_FORMAT=json                           (default: "console")
    # DC_LOG_CONTEXT_MAX_KEYS=50                   (default: 50)
    # DC_LOG_ERROR_FILE=error.log                  (default: "error.log")

Run:
    python -m data_collector.examples run logging/01_splunk_hec
"""

from __future__ import annotations

import uuid

from data_collector.settings.main import LogSettings
from data_collector.utilities.log.main import LoggingService


def _validate_splunk_config(settings: LogSettings) -> bool:
    """Return True when required Splunk settings are present."""
    if not settings.log_to_splunk:
        print("DC_LOG_SPLUNK_ENABLED is not true. Set it to enable Splunk sink.")
        return False
    if not settings.splunk_hec_url:
        print("DC_LOG_SPLUNK_URL is missing.")
        return False
    if not settings.splunk_token:
        print("DC_LOG_SPLUNK_TOKEN is missing.")
        return False
    return True


def _masked_token(token: str | None) -> str:
    """Mask token for safe console output."""
    if not token:
        return "<missing>"
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}...{token[-4:]}"


def main() -> None:
    """Emit real structured logs to Splunk HEC using framework settings."""
    settings = LogSettings(log_to_db=False, log_level=10, log_error_file="error.log")
    if not _validate_splunk_config(settings):
        return

    print("=== Effective Splunk settings ===")
    print(f"  URL: {settings.splunk_hec_url}")
    print(f"  Token: {_masked_token(settings.splunk_token)}")
    print(f"  TLS verify: {settings.splunk_verify_tls}")
    print(f"  CA bundle: {settings.splunk_ca_bundle or '<none>'}")
    print(f"  Index: {settings.splunk_index}")
    print(f"  Sourcetype: {settings.splunk_sourcetype}")
    print(f"  Format: {settings.log_format}")
    print(f"  Error file: {settings.log_error_file}")

    service = LoggingService(logger_name="examples.logging.splunk_hec", settings=settings)

    try:
        logger = service.configure_logger()
        logger = logger.bind(
            app_id="example_logging_app",
            runtime=uuid.uuid4().hex,
        )

        print("\n=== Sending events ===")
        logger.info("Splunk logging example started", example="logging/01_splunk_hec")
        logger.warning("Demo warning event", retries_left=1, target="splunk")
        logger.error("Demo error event", operation="hec_send", status="simulated")
        logger.info(
            "Event with rich context",
            component="ingestion",
            batch_size=500,
            source_system="erp",
        )
        logger.info("Splunk logging example finished", success=True)
        print("Events emitted. Check Splunk index and/or local error.log for sink failures.")
    finally:
        service.stop()


if __name__ == "__main__":
    main()
