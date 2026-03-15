"""Tests for the cross-platform service module."""

from __future__ import annotations

import sys

from data_collector.dramatiq.service import build_dramatiq_command, generate_systemd_unit
from data_collector.settings.dramatiq import DramatiqSettings


class TestBuildDramatiqCommand:
    """Tests for CLI command construction."""

    def test_default_command(self) -> None:
        settings = DramatiqSettings()
        command = build_dramatiq_command(settings)

        assert command[0] == sys.executable
        assert command[1:3] == ["-m", "data_collector.dramatiq.cli_wrapper"]
        assert "data_collector.dramatiq.actors" in command
        assert "-p" in command
        assert "-t" in command

    def test_custom_processes_and_workers(self) -> None:
        settings = DramatiqSettings(processes=4, workers=8)
        command = build_dramatiq_command(settings)

        process_index = command.index("-p")
        assert command[process_index + 1] == "4"

        thread_index = command.index("-t")
        assert command[thread_index + 1] == "8"

    def test_no_queue_flag_when_empty(self) -> None:
        settings = DramatiqSettings(queues="")
        command = build_dramatiq_command(settings)
        assert "-Q" not in command

    def test_single_queue(self) -> None:
        settings = DramatiqSettings(queues="dc_pdf_extract")
        command = build_dramatiq_command(settings)

        queue_index = command.index("-Q")
        assert command[queue_index + 1] == "dc_pdf_extract"

    def test_multiple_queues(self) -> None:
        settings = DramatiqSettings(queues="dc_pdf_extract,dc_eoglasna_prepare")
        command = build_dramatiq_command(settings)

        queue_flags = [i for i, value in enumerate(command) if value == "-Q"]
        assert len(queue_flags) == 2
        queue_names = [command[i + 1] for i in queue_flags]
        assert "dc_pdf_extract" in queue_names
        assert "dc_eoglasna_prepare" in queue_names

    def test_whitespace_in_queues_is_stripped(self) -> None:
        settings = DramatiqSettings(queues=" dc_queue_1 , dc_queue_2 ")
        command = build_dramatiq_command(settings)

        queue_flags = [i for i, value in enumerate(command) if value == "-Q"]
        queue_names = [command[i + 1] for i in queue_flags]
        assert "dc_queue_1" in queue_names
        assert "dc_queue_2" in queue_names

    def test_uses_defaults_when_no_settings(self) -> None:
        command = build_dramatiq_command()

        process_index = command.index("-p")
        assert command[process_index + 1] == "1"

        thread_index = command.index("-t")
        assert command[thread_index + 1] == "4"


class TestGenerateSystemdUnit:
    """Tests for systemd unit file generation."""

    def test_contains_unit_section(self) -> None:
        unit = generate_systemd_unit()
        assert "[Unit]" in unit
        assert "Description=Data Collector Dramatiq Workers" in unit

    def test_contains_service_section(self) -> None:
        unit = generate_systemd_unit()
        assert "[Service]" in unit
        assert "Type=simple" in unit
        assert "Restart=on-failure" in unit
        assert "RestartSec=10" in unit

    def test_contains_install_section(self) -> None:
        unit = generate_systemd_unit()
        assert "[Install]" in unit
        assert "WantedBy=multi-user.target" in unit

    def test_exec_start_uses_current_python(self) -> None:
        unit = generate_systemd_unit()
        assert sys.executable in unit

    def test_exec_start_includes_actor_module(self) -> None:
        unit = generate_systemd_unit()
        assert "data_collector.dramatiq.actors" in unit

    def test_depends_on_rabbitmq(self) -> None:
        unit = generate_systemd_unit()
        assert "rabbitmq-server.service" in unit

    def test_graceful_shutdown_signal(self) -> None:
        unit = generate_systemd_unit()
        assert "KillSignal=SIGTERM" in unit

    def test_custom_settings_reflected(self) -> None:
        settings = DramatiqSettings(processes=3, workers=6)
        unit = generate_systemd_unit(settings)
        assert "-p 3" in unit
        assert "-t 6" in unit

    def test_queue_filtering_in_exec_start(self) -> None:
        settings = DramatiqSettings(queues="dc_pdf_extract")
        unit = generate_systemd_unit(settings)
        assert "-Q dc_pdf_extract" in unit
