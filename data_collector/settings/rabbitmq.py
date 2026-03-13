"""Pydantic settings for RabbitMQ connection and reliability configuration."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class RabbitMQSettings(BaseSettings):
    """RabbitMQ connection settings loaded from environment variables.

    Environment variables follow the ``DC_RABBIT_`` prefix pattern.

    Connection settings control the broker address, credentials, and queue
    identity. Reliability settings control heartbeat, connection timeout,
    and automatic reconnection behaviour with bounded exponential backoff.

    Examples:
        From environment variables::

            settings = RabbitMQSettings()
            connection = RabbitMQConnection(settings)

        Direct construction (testing, overrides)::

            settings = RabbitMQSettings(
                rabbit_host="rabbitmq.internal",
                rabbit_port=5672,
                rabbit_queue="dc_manager_eu",
            )
    """

    model_config = SettingsConfigDict(env_prefix="DC_RABBIT_")

    # -- Connection --
    rabbit_host: str = "localhost"
    rabbit_port: int = 5672
    rabbit_username: str = "guest"
    rabbit_password: str = "guest"
    rabbit_queue: str = "dc_manager"
    rabbit_prefetch: int = 1

    # -- Reliability --
    rabbit_heartbeat: int = 600
    rabbit_connection_timeout: int = 10
    rabbit_reconnect_max_attempts: int = 5
    rabbit_reconnect_base_delay: int = 1
    rabbit_reconnect_max_delay: int = 30
