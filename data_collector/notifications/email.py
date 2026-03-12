"""Email (SMTP) notification channel."""

from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage

from data_collector.notifications.models import Notification
from data_collector.notifications.notifier import BaseNotifier

logger = logging.getLogger(__name__)


class EmailNotifier(BaseNotifier):
    """Send alerts via SMTP email.

    Builds a plain-text email with severity prefix in the subject line
    and structured notification details in the body.

    Args:
        host: SMTP server hostname.
        port: SMTP port (587 for TLS, 465 for SSL, 25 for plain).
        username: SMTP authentication username.
        password: SMTP authentication password.
        sender_address: "From" email address.
        recipient_addresses: Comma-separated recipient email addresses.
        use_tls: Whether to use STARTTLS.
        timeout: SMTP connection timeout in seconds.
    """

    CHANNEL_NAME: str = "email"

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        sender_address: str,
        recipient_addresses: str,
        *,
        use_tls: bool = True,
        timeout: int = 10,
    ) -> None:
        super().__init__(self.CHANNEL_NAME)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.sender_address = sender_address
        self.recipient_addresses = recipient_addresses
        self.use_tls = use_tls
        self.timeout = timeout

    def send(self, notification: Notification) -> bool:
        """Send a notification via SMTP email.

        Args:
            notification: The notification payload to deliver.

        Returns:
            True on successful delivery, False on failure.
        """
        message = self._build_email_message(notification)
        try:
            with smtplib.SMTP(self.host, self.port, timeout=self.timeout) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.username, self.password)
                server.send_message(message)
            return True
        except (smtplib.SMTPException, OSError):
            logger.exception("Failed to send email notification")
            return False

    def is_configured(self) -> bool:
        """Check whether all required SMTP settings are present.

        Returns:
            True if host, username, password, sender, and recipients are set.
        """
        return all([
            self.host,
            self.username,
            self.password,
            self.sender_address,
            self.recipient_addresses,
        ])

    def _build_email_message(self, notification: Notification) -> EmailMessage:
        """Build an EmailMessage from a notification.

        Args:
            notification: The notification to format.

        Returns:
            Constructed EmailMessage ready for sending.
        """
        title = self.format_title(notification)

        body_lines = [
            title,
            "=" * len(title),
            "",
            notification.message,
            "",
            f"Time: {notification.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        ]

        if notification.app_id:
            body_lines.append(f"App: {notification.app_id}")

        if notification.metadata:
            body_lines.append("")
            for key, value in notification.metadata.items():
                body_lines.append(f"{key}: {value}")

        message = EmailMessage()
        message["Subject"] = f"[Data Collector] {title}"
        message["From"] = self.sender_address
        message["To"] = self.recipient_addresses
        message.set_content("\n".join(body_lines))

        return message
