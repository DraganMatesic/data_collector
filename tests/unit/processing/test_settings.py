"""Tests for ProcessingSettings Pydantic configuration."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from data_collector.settings.processing import ProcessingSettings


class TestProcessingSettingsDefaults:
    """Tests for default field values."""

    def test_default_values(self) -> None:
        settings = ProcessingSettings()
        assert settings.text_threshold == 0.9
        assert settings.min_text_length == 50
        assert settings.tesseract_language == "eng"
        assert settings.tesseract_cmd == ""
        assert settings.min_dpi == 300
        assert settings.preprocessing_enabled is True
        assert settings.clahe_clip_limit == 2.0
        assert settings.clahe_tile_size == 8


class TestProcessingSettingsEnvLoading:
    """Tests for environment variable loading."""

    def test_env_var_loading(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_PROCESSING_TEXT_THRESHOLD", "0.8")
        settings = ProcessingSettings()
        assert settings.text_threshold == 0.8

    def test_env_prefix(self) -> None:
        assert ProcessingSettings.model_config.get("env_prefix") == "DC_PROCESSING_"


class TestProcessingSettingsDirectConstruction:
    """Tests for direct keyword construction."""

    def test_direct_construction(self) -> None:
        settings = ProcessingSettings(text_threshold=0.8, tesseract_language="eng+hrv", min_dpi=200)
        assert settings.text_threshold == 0.8
        assert settings.tesseract_language == "eng+hrv"
        assert settings.min_dpi == 200


class TestTextThresholdValidation:
    """Tests for text_threshold field boundaries."""

    def test_text_threshold_too_low(self) -> None:
        with pytest.raises(ValidationError):
            ProcessingSettings(text_threshold=-0.1)

    def test_text_threshold_too_high(self) -> None:
        with pytest.raises(ValidationError):
            ProcessingSettings(text_threshold=1.5)

    def test_text_threshold_boundary_zero(self) -> None:
        settings = ProcessingSettings(text_threshold=0.0)
        assert settings.text_threshold == 0.0

    def test_text_threshold_boundary_one(self) -> None:
        settings = ProcessingSettings(text_threshold=1.0)
        assert settings.text_threshold == 1.0


class TestMinTextLengthValidation:
    """Tests for min_text_length field validation."""

    def test_min_text_length_too_low(self) -> None:
        with pytest.raises(ValidationError):
            ProcessingSettings(min_text_length=0)


class TestMinDpiValidation:
    """Tests for min_dpi field validation."""

    def test_min_dpi_too_low(self) -> None:
        with pytest.raises(ValidationError):
            ProcessingSettings(min_dpi=50)


class TestClaheValidation:
    """Tests for CLAHE-related field validation."""

    def test_clahe_clip_limit_zero(self) -> None:
        with pytest.raises(ValidationError):
            ProcessingSettings(clahe_clip_limit=0)

    def test_clahe_tile_size_too_small(self) -> None:
        with pytest.raises(ValidationError):
            ProcessingSettings(clahe_tile_size=1)
