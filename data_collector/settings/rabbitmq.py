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
                host="rabbitmq.internal",
                port=5672,
                queue="dc_manager_eu",
            )
    """

    model_config = SettingsConfigDict(env_prefix="DC_RABBIT_")

    # -- Connection --
    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"
    queue: str = "dc_manager"
    prefetch: int = 1

    # -- Reliability --
    heartbeat: int = 600
    connection_timeout: int = 10
    reconnect_max_attempts: int = 5
    reconnect_base_delay: int = 1
    reconnect_max_delay: int = 30
