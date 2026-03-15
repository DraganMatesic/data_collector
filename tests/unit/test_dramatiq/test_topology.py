"""Tests for pipeline topology dataclasses and topic module discovery."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest
from pika.exchange_type import ExchangeType

from data_collector.dramatiq.topic.base import (
    DEAD_LETTERS_QUEUE,
    OCR_TOPIC_EXCHANGE,
    UNROUTABLE_EXCHANGE,
    RegularQueue,
    TopicExchange,
    TopicExchangeQueue,
    get_topic_modules,
)


class TestTopicExchange:
    """Tests for TopicExchange dataclass."""

    def test_defaults(self) -> None:
        exchange = TopicExchange(name="test_exchange")
        assert exchange.name == "test_exchange"
        assert exchange.durable is True
        assert exchange.exchange_type == ExchangeType.topic
        assert exchange.arguments == {}

    def test_custom_values(self) -> None:
        exchange = TopicExchange(
            name="my_exchange",
            durable=False,
            exchange_type=ExchangeType.fanout,
            arguments={"alternate-exchange": "fallback"},
        )
        assert exchange.name == "my_exchange"
        assert exchange.durable is False
        assert exchange.exchange_type == ExchangeType.fanout
        assert exchange.arguments == {"alternate-exchange": "fallback"}

    def test_frozen(self) -> None:
        exchange = TopicExchange(name="test")
        with pytest.raises(AttributeError):
            exchange.name = "modified"  # type: ignore[misc]


class TestTopicExchangeQueue:
    """Tests for TopicExchangeQueue dataclass."""

    def test_defaults(self) -> None:
        queue = TopicExchangeQueue(name="test_queue", actor_name="test_actor")
        assert queue.name == "test_queue"
        assert queue.actor_name == "test_actor"
        assert queue.durable is True
        assert queue.exchange_name == ""
        assert queue.routing_key == ""
        assert queue.actor_path == ""

    def test_full_construction(self) -> None:
        queue = TopicExchangeQueue(
            name="dc_pdf_extract",
            actor_name="process_pdf",
            exchange_name="dc_ocr_topic",
            routing_key="ocr.pdf.extract",
            actor_path="data_collector.dramatiq.workers.pdf_processor",
        )
        assert queue.name == "dc_pdf_extract"
        assert queue.actor_name == "process_pdf"
        assert queue.exchange_name == "dc_ocr_topic"
        assert queue.routing_key == "ocr.pdf.extract"
        assert queue.actor_path == "data_collector.dramatiq.workers.pdf_processor"

    def test_frozen(self) -> None:
        queue = TopicExchangeQueue(name="test", actor_name="act")
        with pytest.raises(AttributeError):
            queue.name = "modified"  # type: ignore[misc]


class TestRegularQueue:
    """Tests for RegularQueue dataclass."""

    def test_defaults(self) -> None:
        queue = RegularQueue(name="test_queue")
        assert queue.name == "test_queue"
        assert queue.durable is True
        assert queue.actor_name == ""
        assert queue.actor_path == ""

    def test_full_construction(self) -> None:
        queue = RegularQueue(
            name="dc_dead_letters",
            actor_name="log_dead_letter",
            actor_path="data_collector.dramatiq.workers.dead_letters",
        )
        assert queue.name == "dc_dead_letters"
        assert queue.actor_name == "log_dead_letter"
        assert queue.actor_path == "data_collector.dramatiq.workers.dead_letters"


class TestPreDefinedConstants:
    """Tests for pre-defined exchange and queue constants."""

    def test_unroutable_exchange(self) -> None:
        assert UNROUTABLE_EXCHANGE.name == "dc_unroutable"
        assert UNROUTABLE_EXCHANGE.exchange_type == ExchangeType.fanout
        assert UNROUTABLE_EXCHANGE.durable is True

    def test_ocr_topic_exchange(self) -> None:
        assert OCR_TOPIC_EXCHANGE.name == "dc_ocr_topic"
        assert OCR_TOPIC_EXCHANGE.exchange_type == ExchangeType.topic
        assert OCR_TOPIC_EXCHANGE.arguments == {"alternate-exchange": "dc_unroutable"}

    def test_dead_letters_queue(self) -> None:
        assert DEAD_LETTERS_QUEUE.name == "dc_dead_letters"
        assert DEAD_LETTERS_QUEUE.actor_name == "log_dead_letter"
        assert DEAD_LETTERS_QUEUE.actor_path == "data_collector.dramatiq.workers.dead_letters"


class TestDiscoverTopicModules:
    """Tests for get_topic_modules() convention-based discovery."""

    @pytest.fixture(autouse=True)
    def clear_topic_modules_cache(self) -> Generator[None]:
        """Clear lru_cache before and after each test."""
        get_topic_modules.cache_clear()
        yield
        get_topic_modules.cache_clear()

    @patch("data_collector.dramatiq.topic.base._package_root")
    def test_core_module_always_first(self, mock_root: Path, tmp_path: Path) -> None:
        mock_root.return_value = tmp_path
        result = get_topic_modules()
        assert result[0] == "data_collector.dramatiq.topic.base"

    @patch("data_collector.dramatiq.topic.base._package_root")
    def test_discovers_topics_in_app_namespace(self, mock_root: Path, tmp_path: Path) -> None:
        mock_root.return_value = tmp_path
        app_dir = tmp_path / "croatia" / "gazette" / "ocr"
        app_dir.mkdir(parents=True)
        (app_dir / "topics.py").write_text("# topic definitions")

        result = get_topic_modules()
        assert "data_collector.croatia.gazette.ocr.topics" in result

    @patch("data_collector.dramatiq.topic.base._package_root")
    def test_excludes_framework_directories(self, mock_root: Path, tmp_path: Path) -> None:
        mock_root.return_value = tmp_path
        for framework_directory in ("dramatiq", "enums", "settings", "utilities", "tables"):
            directory = tmp_path / framework_directory / "sub"
            directory.mkdir(parents=True)
            (directory / "topics.py").write_text("# should be excluded")

        result = get_topic_modules()
        assert len(result) == 1  # only core module

    @patch("data_collector.dramatiq.topic.base._package_root")
    def test_excludes_pycache(self, mock_root: Path, tmp_path: Path) -> None:
        mock_root.return_value = tmp_path
        cache_dir = tmp_path / "croatia" / "__pycache__"
        cache_dir.mkdir(parents=True)
        (cache_dir / "topics.py").write_text("# cached bytecode artifact")

        result = get_topic_modules()
        assert len(result) == 1  # only core module

    @patch("data_collector.dramatiq.topic.base._package_root")
    def test_sorted_deterministic(self, mock_root: Path, tmp_path: Path) -> None:
        mock_root.return_value = tmp_path
        for country in ("croatia", "australia", "germany"):
            directory = tmp_path / country / "parent" / "app"
            directory.mkdir(parents=True)
            (directory / "topics.py").write_text("# topics")

        result = get_topic_modules()
        discovered = result[1:]  # skip core module
        assert discovered == sorted(discovered)
        assert "australia" in discovered[0]
        assert "germany" in discovered[-1]

    @patch("data_collector.dramatiq.topic.base._package_root")
    def test_empty_when_no_apps(self, mock_root: Path, tmp_path: Path) -> None:
        mock_root.return_value = tmp_path
        result = get_topic_modules()
        assert result == ["data_collector.dramatiq.topic.base"]
