"""Tests for the Dramatiq worker entry point module."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

from data_collector.dramatiq.actors import DramatiqProcessInitializer, _bootstrap_logging


class TestBootstrapLogging:
    """Tests for LoggingService bootstrap initialization."""

    @patch("data_collector.dramatiq.actors.structlog.contextvars.bind_contextvars")
    @patch("data_collector.dramatiq.actors.LoggingService")
    def test_configures_logger_with_db_engine(
        self,
        mock_logging_service_class: MagicMock,
        mock_bind_contextvars: MagicMock,
    ) -> None:
        mock_database = MagicMock()
        mock_service_instance = MagicMock()
        mock_logging_service_class.return_value = mock_service_instance

        _bootstrap_logging(mock_database, "test_runtime_id")

        mock_logging_service_class.assert_called_once()
        call_kwargs = mock_logging_service_class.call_args
        assert call_kwargs[0][0] == "data_collector"
        assert call_kwargs[1]["db_engine"] is mock_database.engine
        mock_service_instance.configure_logger.assert_called_once()
        mock_bind_contextvars.assert_called_once()


class TestDramatiqProcessInitializer:
    """Tests for the @fun_watch-based initialization class."""

    @patch("data_collector.dramatiq.actors.DramatiqBroker")
    @patch("data_collector.dramatiq.actors.DramatiqSettings")
    def test_initialize_broker_creates_broker_with_load_true(
        self,
        mock_dramatiq_settings: MagicMock,
        mock_broker_class: MagicMock,
    ) -> None:
        mock_settings = MagicMock()
        mock_dramatiq_settings.return_value = mock_settings
        mock_rabbitmq_settings = MagicMock()

        initializer = DramatiqProcessInitializer(
            app_id="test_app_id",
            runtime="test_runtime",
            structured_logger=MagicMock(),
        )
        initializer.initialize_broker(mock_rabbitmq_settings)

        mock_broker_class.assert_called_once_with(
            mock_rabbitmq_settings,
            dramatiq_settings=mock_settings,
            load=True,
        )

    @patch("data_collector.dramatiq.actors.importlib.import_module")
    @patch("data_collector.dramatiq.actors.get_topic_modules", return_value=["test.topic.module"])
    def test_discover_actors_imports_actor_paths(
        self, _mock_get_topic_modules: MagicMock, mock_import: MagicMock,
    ) -> None:
        from data_collector.dramatiq.topic.base import RegularQueue

        mock_queue = RegularQueue(
            name="dc_test_queue",
            actor_name="test_actor",
            actor_path="test.workers.my_actor",
        )

        mock_topic_module = MagicMock()
        mock_topic_module.__dir__ = MagicMock(return_value=["TEST_QUEUE"])
        type(mock_topic_module).TEST_QUEUE = mock_queue

        mock_import.side_effect = lambda module_path: {
            "test.topic.module": mock_topic_module,
            "test.workers.my_actor": MagicMock(),
        }.get(module_path, MagicMock())

        initializer = DramatiqProcessInitializer(
            app_id="test_app_id",
            runtime="test_runtime",
            structured_logger=MagicMock(),
        )
        initializer.discover_actors()

        assert call("test.workers.my_actor") in mock_import.call_args_list

    @patch("data_collector.dramatiq.actors.importlib.import_module")
    @patch("data_collector.dramatiq.actors.get_topic_modules", return_value=["test.topic.module"])
    def test_discover_actors_skips_queues_without_actor_path(
        self, _mock_get_topic_modules: MagicMock, mock_import: MagicMock,
    ) -> None:
        from data_collector.dramatiq.topic.base import RegularQueue

        mock_queue = RegularQueue(name="dc_test_queue")

        mock_topic_module = MagicMock()
        mock_topic_module.__dir__ = MagicMock(return_value=["TEST_QUEUE"])
        type(mock_topic_module).TEST_QUEUE = mock_queue

        mock_import.return_value = mock_topic_module

        initializer = DramatiqProcessInitializer(
            app_id="test_app_id",
            runtime="test_runtime",
            structured_logger=MagicMock(),
        )
        initializer.discover_actors()

        mock_import.assert_called_once_with("test.topic.module")

    @patch("data_collector.dramatiq.actors.importlib.import_module")
    @patch("data_collector.dramatiq.actors.get_topic_modules", return_value=["nonexistent.module"])
    def test_discover_actors_handles_missing_topic_module(
        self, _mock_get_topic_modules: MagicMock, mock_import: MagicMock,
    ) -> None:
        mock_import.side_effect = ImportError("No module")

        initializer = DramatiqProcessInitializer(
            app_id="test_app_id",
            runtime="test_runtime",
            structured_logger=MagicMock(),
        )
        initializer.discover_actors()  # Should not raise

    @patch("data_collector.dramatiq.actors.importlib.import_module")
    @patch("data_collector.dramatiq.actors.get_topic_modules", return_value=["test.topic.module"])
    def test_discover_actors_deduplicates_imports(
        self, _mock_get_topic_modules: MagicMock, mock_import: MagicMock,
    ) -> None:
        from data_collector.dramatiq.topic.base import TopicExchangeQueue

        mock_queue_1 = TopicExchangeQueue(
            name="dc_queue_1",
            actor_name="actor_a",
            actor_path="test.workers.shared",
        )
        mock_queue_2 = TopicExchangeQueue(
            name="dc_queue_2",
            actor_name="actor_b",
            actor_path="test.workers.shared",
        )

        mock_topic_module = MagicMock()
        mock_topic_module.__dir__ = MagicMock(return_value=["QUEUE_1", "QUEUE_2"])
        type(mock_topic_module).QUEUE_1 = mock_queue_1
        type(mock_topic_module).QUEUE_2 = mock_queue_2

        mock_import.side_effect = lambda module_path: {
            "test.topic.module": mock_topic_module,
            "test.workers.shared": MagicMock(),
        }[module_path]

        initializer = DramatiqProcessInitializer(
            app_id="test_app_id",
            runtime="test_runtime",
            structured_logger=MagicMock(),
        )
        initializer.discover_actors()

        shared_calls = [c for c in mock_import.call_args_list if c == call("test.workers.shared")]
        assert len(shared_calls) == 1
