"""Send a test alert via SMTP email.

Demonstrates:
    - EmailNotifier instantiation from environment variables
    - Sending a test notification with plain-text email formatting
    - TLS support
    - Checking delivery result

Setup:
    Use any SMTP relay. For SendGrid:
    1. Create a SendGrid account at https://sendgrid.com.
    2. Go to Settings > API Keys and create an API key with "Mail Send" permission.
    3. Use smtp.sendgrid.net:587 with username "apikey" (literal string)
       and the API key as the password.
    4. Verify a Sender Identity under Settings > Sender Authentication.

    For Amazon SES, Mailgun, Postmark, or a corporate relay, use the SMTP
    hostname, port, and credentials provided by the service or IT.

Requires:
    DC_NOTIFICATION_SMTP_HOST, DC_NOTIFICATION_SMTP_PORT,
    DC_NOTIFICATION_SMTP_USERNAME, DC_NOTIFICATION_SMTP_PASSWORD,
    DC_NOTIFICATION_SMTP_FROM, DC_NOTIFICATION_SMTP_TO
    environment variables.

Run:
    python -m data_collector.examples run notifications/03_email
"""

from __future__ import annotations

import os

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.email import EmailNotifier
from data_collector.notifications.models import Notification

_REQUIRED_ENV = (
    "DC_NOTIFICATION_SMTP_HOST",
    "DC_NOTIFICATION_SMTP_USERNAME",
    "DC_NOTIFICATION_SMTP_PASSWORD",
    "DC_NOTIFICATION_SMTP_FROM",
    "DC_NOTIFICATION_SMTP_TO",
)


def main() -> None:
    """Send a test notification via SMTP email."""
    missing = [variable for variable in _REQUIRED_ENV if not os.environ.get(variable)]
    if missing:
        print(f"Skipping: env vars not set: {', '.join(missing)}")
        return

    notifier = EmailNotifier(
        host=os.environ["DC_NOTIFICATION_SMTP_HOST"],
        port=int(os.environ.get("DC_NOTIFICATION_SMTP_PORT", "587")),
        username=os.environ["DC_NOTIFICATION_SMTP_USERNAME"],
        password=os.environ["DC_NOTIFICATION_SMTP_PASSWORD"],
        sender_address=os.environ["DC_NOTIFICATION_SMTP_FROM"],
        recipient_addresses=os.environ["DC_NOTIFICATION_SMTP_TO"],
        use_tls=os.environ.get("DC_NOTIFICATION_SMTP_USE_TLS", "true").lower() == "true",
    )

    if not notifier.is_configured():
        print("Email notifier is not configured (missing required SMTP settings)")
        return

    notification = Notification(
        severity=AlertSeverity.INFO,
        title="data_collector/test",
        message="This is a test notification from Data Collector.",
        metadata={"source": "03_email.py example"},
    )

    recipient_display = os.environ["DC_NOTIFICATION_SMTP_TO"]
    print(f"Sending test notification to {recipient_display}...")
    success = notifier.send(notification)
    print(f"Result: {'SUCCESS' if success else 'FAILED'}")


if __name__ == "__main__":
    main()
