"""Command message model for RabbitMQ command distribution."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from data_collector.enums.commands import CmdName


@dataclass(frozen=True)
class CommandMessage:
    """Immutable command message published to the RabbitMQ command exchange.

    Represents a single command issued to a target application via the
    ``dc_commands`` exchange. Serialises to the JSON wire format defined
    in the RabbitMQ specification.

    Args:
        app_id: Target application identifier (64-char SHA-256 hex).
        command: Command type from CmdName enum.
        issued_by: Email or identifier of the command issuer.
        timestamp: When the command was issued. Defaults to current UTC time.
        args: Optional key-value arguments forwarded to the target application.
    """

    app_id: str
    command: CmdName
    issued_by: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    args: dict[str, Any] | None = None

    def to_json_bytes(self) -> bytes:
        """Serialize to JSON bytes for RabbitMQ publishing.

        Returns:
            UTF-8 encoded JSON matching the command protocol wire format:
            ``{"app_id": "...", "command": 1, "issued_by": "...",
            "timestamp": "...", "args": {...}}``.
        """
        payload: dict[str, Any] = {
            "app_id": self.app_id,
            "command": int(self.command),
            "issued_by": self.issued_by,
            "timestamp": self.timestamp.isoformat(),
        }
        if self.args is not None:
            payload["args"] = self.args
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, data: bytes) -> CommandMessage:
        """Deserialize from JSON bytes received from RabbitMQ.

        Args:
            data: UTF-8 encoded JSON payload.

        Returns:
            Parsed CommandMessage instance.

        Raises:
            ValueError: If required fields are missing or command value is invalid.
            json.JSONDecodeError: If data is not valid JSON.
        """
        parsed = json.loads(data)

        missing_fields = {"app_id", "command", "issued_by"} - parsed.keys()
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(sorted(missing_fields))}")

        try:
            command = CmdName(parsed["command"])
        except ValueError:
            raise ValueError(f"Invalid command value: {parsed['command']}") from None

        timestamp_raw = parsed.get("timestamp")
        timestamp = datetime.fromisoformat(timestamp_raw) if timestamp_raw is not None else datetime.now(UTC)

        return cls(
            app_id=parsed["app_id"],
            command=command,
            issued_by=parsed["issued_by"],
            timestamp=timestamp,
            args=parsed.get("args"),
        )
