"""Pydantic settings for PDF/OCR processing configuration."""

from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProcessingSettings(BaseSettings):
    """PDF/OCR processing settings loaded from environment variables.

    Environment variables follow the ``DC_PROCESSING_`` prefix pattern:

        DC_PROCESSING_TEXT_THRESHOLD         -- Fraction of pages (0.0-1.0) that must
                                                have extractable text for a PDF to be
                                                classified as text-based. Default: 0.9.
        DC_PROCESSING_MIN_TEXT_LENGTH        -- Minimum character count for a page to be
                                                considered as having meaningful text.
                                                Default: 50.
        DC_PROCESSING_TESSERACT_LANGUAGE     -- Tesseract language string (e.g., ``"eng"``,
                                                ``"eng+hrv"``). Default: ``"eng"``.
        DC_PROCESSING_TESSERACT_CMD          -- Path to the Tesseract executable. When
                                                empty, pytesseract uses its default PATH
                                                lookup. Default: ``""`` (empty).
        DC_PROCESSING_MIN_DPI                -- Minimum DPI threshold for OCR preprocessing.
                                                Images below this DPI are upscaled.
                                                Default: 300.
        DC_PROCESSING_PREPROCESSING_ENABLED  -- Enable the full preprocessing pipeline
                                                (grayscale, upscale, denoise, contrast,
                                                deskew, binarize, morphology, border)
                                                before OCR. Default: True.
        DC_PROCESSING_CLAHE_CLIP_LIMIT       -- CLAHE contrast clip limit. Controls how
                                                much contrast enhancement is applied.
                                                Default: 2.0.
        DC_PROCESSING_CLAHE_TILE_SIZE        -- CLAHE tile grid size. Determines the size
                                                of local regions for adaptive histogram
                                                equalization. Default: 8.

    Examples:
        From environment variables::

            settings = ProcessingSettings()
            doc_type = classify_pdf(path, threshold=settings.text_threshold)

        Direct construction (testing)::

            settings = ProcessingSettings(text_threshold=0.8, tesseract_language="eng+hrv")
    """

    model_config = SettingsConfigDict(env_prefix="DC_PROCESSING_")

    # -- PDF classification --
    text_threshold: float = 0.9
    min_text_length: int = 50

    # -- Tesseract configuration --
    tesseract_language: str = "eng"
    tesseract_cmd: str = ""

    # -- Preprocessing --
    min_dpi: int = 300
    preprocessing_enabled: bool = True

    # -- CLAHE contrast enhancement --
    clahe_clip_limit: float = 2.0
    clahe_tile_size: int = 8

    @field_validator("text_threshold")
    @classmethod
    def validate_text_threshold(cls, value: float) -> float:
        """Validate that text_threshold is between 0.0 and 1.0 inclusive."""
        if not 0.0 <= value <= 1.0:
            message = f"text_threshold must be between 0.0 and 1.0, got {value}"
            raise ValueError(message)
        return value

    @field_validator("min_text_length")
    @classmethod
    def validate_min_text_length(cls, value: int) -> int:
        """Validate that min_text_length is positive."""
        if value < 1:
            message = f"min_text_length must be >= 1, got {value}"
            raise ValueError(message)
        return value

    @field_validator("min_dpi")
    @classmethod
    def validate_min_dpi(cls, value: int) -> int:
        """Validate that min_dpi is a reasonable DPI value."""
        if value < 72:
            message = f"min_dpi must be >= 72, got {value}"
            raise ValueError(message)
        return value

    @field_validator("clahe_clip_limit")
    @classmethod
    def validate_clahe_clip_limit(cls, value: float) -> float:
        """Validate that clahe_clip_limit is positive."""
        if value <= 0:
            message = f"clahe_clip_limit must be > 0, got {value}"
            raise ValueError(message)
        return value

    @field_validator("clahe_tile_size")
    @classmethod
    def validate_clahe_tile_size(cls, value: int) -> int:
        """Validate that clahe_tile_size is at least 2."""
        if value < 2:
            message = f"clahe_tile_size must be >= 2, got {value}"
            raise ValueError(message)
        return value
