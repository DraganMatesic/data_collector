"""Tests for CaptchaSettings Pydantic configuration."""

import pytest

from data_collector.settings.captcha import CaptchaSettings


class TestCaptchaSettingsFromEnv:
    """Tests for environment variable loading."""

    def test_loads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_CAPTCHA_API_KEY", "test-api-key-123")
        monkeypatch.setenv("DC_CAPTCHA_TIMEOUT", "60")
        monkeypatch.setenv("DC_CAPTCHA_MAX_RETRIES", "3")
        monkeypatch.setenv("DC_CAPTCHA_POLL_INTERVAL", "10")

        settings = CaptchaSettings()  # type: ignore[call-arg]

        assert settings.api_key == "test-api-key-123"
        assert settings.timeout == 60
        assert settings.max_retries == 3
        assert settings.poll_interval == 10

    def test_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_CAPTCHA_API_KEY", "key-only")

        settings = CaptchaSettings()  # type: ignore[call-arg]

        assert settings.api_key == "key-only"
        assert settings.timeout == 120
        assert settings.max_retries == 2
        assert settings.poll_interval == 5

    def test_missing_api_key_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DC_CAPTCHA_API_KEY", raising=False)
        with pytest.raises(ValueError):
            CaptchaSettings()  # type: ignore[call-arg]

    def test_direct_construction(self) -> None:
        settings = CaptchaSettings(api_key="direct-key", timeout=30)
        assert settings.api_key == "direct-key"
        assert settings.timeout == 30
        assert settings.max_retries == 2
