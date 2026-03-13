"""Tests for the CommandMessage model."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from data_collector.enums.commands import CmdName
from data_collector.messaging.models import CommandMessage


class TestCommandMessage:
    """Tests for CommandMessage construction and properties."""

    def test_construction_with_defaults(self) -> None:
        message = CommandMessage(
            app_id="abc123",
            command=CmdName.START,
            issued_by="admin@example.com",
        )
        assert message.app_id == "abc123"
        assert message.command == CmdName.START
        assert message.issued_by == "admin@example.com"
        assert message.args is None
        assert isinstance(message.timestamp, datetime)

    def test_construction_with_all_fields(self) -> None:
        timestamp = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
        message = CommandMessage(
            app_id="abc123",
            command=CmdName.STOP,
            issued_by="admin@example.com",
            timestamp=timestamp,
            args={"company_id": "456"},
        )
        assert message.command == CmdName.STOP
        assert message.timestamp == timestamp
        assert message.args == {"company_id": "456"}

    def test_frozen(self) -> None:
        message = CommandMessage(
            app_id="abc123",
            command=CmdName.START,
            issued_by="admin@example.com",
        )
        with pytest.raises(AttributeError):
            message.app_id = "changed"  # type: ignore[misc]


class TestCommandMessageSerialization:
    """Tests for JSON serialization and deserialization."""

    def test_to_json_bytes(self) -> None:
        timestamp = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
        message = CommandMessage(
            app_id="abc123",
            command=CmdName.START,
            issued_by="admin@example.com",
            timestamp=timestamp,
        )
        data = json.loads(message.to_json_bytes())
        assert data["app_id"] == "abc123"
        assert data["command"] == 1
        assert data["issued_by"] == "admin@example.com"
        assert "timestamp" in data
        assert "args" not in data

    def test_to_json_bytes_with_args(self) -> None:
        message = CommandMessage(
            app_id="abc123",
            command=CmdName.START,
            issued_by="admin@example.com",
            args={"company_id": "456", "monitoring": True},
        )
        data = json.loads(message.to_json_bytes())
        assert data["args"] == {"company_id": "456", "monitoring": True}

    def test_roundtrip(self) -> None:
        timestamp = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
        original = CommandMessage(
            app_id="abc123",
            command=CmdName.RESTART,
            issued_by="admin@example.com",
            timestamp=timestamp,
            args={"key": "value"},
        )
        restored = CommandMessage.from_json_bytes(original.to_json_bytes())
        assert restored.app_id == original.app_id
        assert restored.command == original.command
        assert restored.issued_by == original.issued_by
        assert restored.timestamp == original.timestamp
        assert restored.args == original.args

    def test_from_json_bytes_without_args(self) -> None:
        payload = json.dumps({
            "app_id": "abc123",
            "command": 2,
            "issued_by": "admin@example.com",
            "timestamp": "2025-01-15T10:30:00+00:00",
        }).encode()
        message = CommandMessage.from_json_bytes(payload)
        assert message.command == CmdName.STOP
        assert message.args is None

    def test_from_json_bytes_without_timestamp(self) -> None:
        payload = json.dumps({
            "app_id": "abc123",
            "command": 1,
            "issued_by": "admin@example.com",
        }).encode()
        message = CommandMessage.from_json_bytes(payload)
        assert message.command == CmdName.START
        assert isinstance(message.timestamp, datetime)

    def test_from_json_bytes_invalid_command(self) -> None:
        payload = json.dumps({
            "app_id": "abc123",
            "command": 99,
            "issued_by": "admin@example.com",
        }).encode()
        with pytest.raises(ValueError, match="Invalid command value"):
            CommandMessage.from_json_bytes(payload)

    def test_from_json_bytes_missing_fields(self) -> None:
        payload = json.dumps({"app_id": "abc123"}).encode()
        with pytest.raises(ValueError, match="Missing required fields"):
            CommandMessage.from_json_bytes(payload)

    def test_from_json_bytes_invalid_json(self) -> None:
        with pytest.raises(json.JSONDecodeError):
            CommandMessage.from_json_bytes(b"not json")

    def test_all_command_types_roundtrip(self) -> None:
        for command in CmdName:
            message = CommandMessage(
                app_id="abc123",
                command=command,
                issued_by="test@example.com",
            )
            restored = CommandMessage.from_json_bytes(message.to_json_bytes())
            assert restored.command == command
