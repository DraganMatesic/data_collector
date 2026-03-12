"""Pydantic settings for captcha provider configuration."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class CaptchaSettings(BaseSettings):
    """Captcha provider settings loaded from environment variables.

    Environment variables follow the ``DC_CAPTCHA_`` prefix pattern:

        DC_CAPTCHA_API_KEY          -- Provider API key (clientKey). Required.
                                       Obtain from the AntiCaptcha dashboard
                                       under Settings > API Key.
        DC_CAPTCHA_TIMEOUT          -- Maximum seconds to wait for a single
                                       captcha solution before raising
                                       CaptchaTimeout. Default: 120.
        DC_CAPTCHA_MAX_RETRIES      -- Number of additional attempts after a
                                       CaptchaTimeout. Total attempts =
                                       1 + max_retries. Default: 2.
        DC_CAPTCHA_POLL_INTERVAL    -- Seconds to wait between consecutive
                                       getTaskResult polling requests.
                                       Default: 5.

    Unlike ProxySettings (which is parameterized per zone), captcha settings
    use a single API key shared across all apps. Each provider typically has
    one account with one API key.

    Examples:
        From environment variables::

            settings = CaptchaSettings()
            provider = AntiCaptchaProvider(
                api_key=settings.api_key,
                request=Request(timeout=settings.timeout),
                timeout=settings.timeout,
                max_retries=settings.max_retries,
                poll_interval=settings.poll_interval,
            )

        Direct construction (testing, overrides)::

            settings = CaptchaSettings(api_key="test-key", timeout=60)
    """

    model_config = SettingsConfigDict(env_prefix="DC_CAPTCHA_")

    api_key: str
    timeout: int = 120
    max_retries: int = 2
    poll_interval: int = 5
