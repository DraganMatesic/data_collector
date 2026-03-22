"""Tests for OCREngine ABC, TesseractEngine, and extract_text_ocr convenience function."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from PIL import Image

from data_collector.processing.models import OCRError, OCRResult
from data_collector.processing.ocr import OCREngine, TesseractEngine, extract_text_ocr


def _make_test_image() -> Image.Image:
    """Create a minimal 100x100 RGB PIL Image for testing."""
    return Image.fromarray(np.zeros((100, 100, 3), dtype=np.uint8))


class TestOCREngineAbstract:
    """Tests for OCREngine as an abstract base class."""

    def test_cannot_instantiate(self) -> None:
        with pytest.raises(TypeError):
            OCREngine()  # type: ignore[abstract]

    def test_subclass_must_implement_methods(self) -> None:
        class IncompleteEngine(OCREngine):
            """Subclass missing required abstract methods."""

            @property
            def engine_name(self) -> str:
                return "incomplete"

        with pytest.raises(TypeError):
            IncompleteEngine()  # type: ignore[abstract]


class TestTesseractEngine:
    """Tests for TesseractEngine with mocked pytesseract."""

    @patch("data_collector.processing.ocr.pytesseract")
    def test_engine_name(self, mock_pytesseract: MagicMock) -> None:
        engine = TesseractEngine()
        assert engine.engine_name == "tesseract"

    @patch("data_collector.processing.ocr.pytesseract")
    def test_basic_extraction(self, mock_pytesseract: MagicMock) -> None:
        mock_pytesseract.image_to_string.return_value = "Hello World"
        mock_pytesseract.image_to_data.return_value = {"conf": [95]}
        mock_pytesseract.Output.DICT = "dict"

        engine = TesseractEngine()
        result = engine.extract_text(_make_test_image())

        assert result.text == "Hello World"
        assert isinstance(result, OCRResult)

    @patch("data_collector.processing.ocr.pytesseract")
    def test_language_parameter(self, mock_pytesseract: MagicMock) -> None:
        mock_pytesseract.image_to_string.return_value = "Tekst"
        mock_pytesseract.image_to_data.return_value = {"conf": [90]}
        mock_pytesseract.Output.DICT = "dict"

        engine = TesseractEngine(language="eng+hrv")
        test_image = _make_test_image()
        engine.extract_text(test_image)

        mock_pytesseract.image_to_string.assert_called_once_with(test_image, lang="eng+hrv")

    @patch("data_collector.processing.ocr.pytesseract")
    def test_tesseract_not_found_raises_ocr_error(self, mock_pytesseract: MagicMock) -> None:
        mock_pytesseract.TesseractNotFoundError = type("TesseractNotFoundError", (Exception,), {})
        mock_pytesseract.image_to_string.side_effect = mock_pytesseract.TesseractNotFoundError("not found")

        engine = TesseractEngine()
        with pytest.raises(OCRError, match="Tesseract binary not found"):
            engine.extract_text(_make_test_image())

    @patch("data_collector.processing.ocr.pytesseract")
    def test_tesseract_error_raises_ocr_error(self, mock_pytesseract: MagicMock) -> None:
        mock_pytesseract.TesseractError = type("TesseractError", (Exception,), {})
        mock_pytesseract.TesseractNotFoundError = type("TesseractNotFoundError", (Exception,), {})
        mock_pytesseract.image_to_string.side_effect = mock_pytesseract.TesseractError("bad image")

        engine = TesseractEngine()
        with pytest.raises(OCRError, match="Tesseract extraction failed"):
            engine.extract_text(_make_test_image())

    @patch("data_collector.processing.ocr.pytesseract")
    def test_confidence_extraction(self, mock_pytesseract: MagicMock) -> None:
        mock_pytesseract.image_to_string.return_value = "Some text"
        mock_pytesseract.image_to_data.return_value = {"conf": [95, 87, -1, 92]}
        mock_pytesseract.Output.DICT = "dict"

        engine = TesseractEngine()
        result = engine.extract_text(_make_test_image())

        expected_confidence = (95 + 87 + 92) / (3 * 100)
        assert result.confidence == pytest.approx(expected_confidence)

    @patch("data_collector.processing.ocr.pytesseract")
    def test_preprocessing_integration(self, mock_pytesseract: MagicMock) -> None:
        mock_pytesseract.image_to_string.return_value = "Preprocessed text"
        mock_pytesseract.image_to_data.return_value = {"conf": [90]}
        mock_pytesseract.Output.DICT = "dict"

        mock_preprocessor = MagicMock()
        mock_preprocessor.run.return_value = _make_test_image()

        engine = TesseractEngine(preprocessor=mock_preprocessor)
        result = engine.extract_text(_make_test_image())

        mock_preprocessor.run.assert_called_once()
        assert result.preprocessed is True

    @patch("data_collector.processing.ocr.pytesseract")
    def test_no_preprocessing(self, mock_pytesseract: MagicMock) -> None:
        mock_pytesseract.image_to_string.return_value = "Raw text"
        mock_pytesseract.image_to_data.return_value = {"conf": [80]}
        mock_pytesseract.Output.DICT = "dict"

        engine = TesseractEngine()
        result = engine.extract_text(_make_test_image())

        assert result.preprocessed is False


class TestExtractTextOcr:
    """Tests for the extract_text_ocr convenience function."""

    @patch("data_collector.processing.ocr.PaddleOCREngine")
    def test_default_engine_is_paddleocr(self, mock_paddle_engine_class: MagicMock) -> None:
        mock_engine_instance = MagicMock()
        mock_engine_instance.engine_name = "paddleocr"
        mock_engine_instance.extract_text.return_value = OCRResult(
            text="paddle output", engine_name="paddleocr", preprocessed=False, confidence=0.9
        )
        mock_paddle_engine_class.return_value = mock_engine_instance

        result = extract_text_ocr(_make_test_image())

        mock_paddle_engine_class.assert_called_once()
        assert result.text == "paddle output"

    def test_custom_engine(self) -> None:
        mock_engine = MagicMock()
        mock_engine.extract_text.return_value = OCRResult(
            text="custom output", engine_name="custom", preprocessed=False, confidence=0.85
        )

        result = extract_text_ocr(_make_test_image(), engine=mock_engine)

        mock_engine.extract_text.assert_called_once()
        assert result.text == "custom output"

    @patch("data_collector.processing.ocr.Image")
    def test_file_path_opens_image(self, mock_image_module: MagicMock) -> None:
        mock_opened_image = MagicMock(spec=Image.Image)
        mock_image_module.open.return_value = mock_opened_image

        mock_engine = MagicMock()
        mock_engine.extract_text.return_value = OCRResult(
            text="from file", engine_name="mock", preprocessed=False, confidence=0.9
        )

        extract_text_ocr("/path/to/image.png", engine=mock_engine)

        mock_image_module.open.assert_called_once()

    @patch("data_collector.processing.ocr.Image")
    def test_file_not_found_raises_ocr_error(self, mock_image_module: MagicMock) -> None:
        mock_image_module.open.side_effect = FileNotFoundError("No such file")

        mock_engine = MagicMock()

        with pytest.raises(OCRError, match="Cannot open image"):
            extract_text_ocr("/nonexistent/image.png", engine=mock_engine)

    @patch("data_collector.processing.ocr.Image")
    def test_extract_text_ocr_with_path_object(self, mock_image_module: MagicMock) -> None:
        """7.1.12: Passing a Path object (not str) to extract_text_ocr calls Image.open."""
        mock_opened_image = MagicMock(spec=Image.Image)
        mock_image_module.open.return_value = mock_opened_image

        mock_engine = MagicMock()
        mock_engine.extract_text.return_value = OCRResult(
            text="from path object", engine_name="mock", preprocessed=False, confidence=0.9
        )

        extract_text_ocr(Path("/path/to/image.png"), engine=mock_engine)

        mock_image_module.open.assert_called_once()


class TestTesseractConfidence:
    """Tests for Tesseract confidence edge cases."""

    @patch("data_collector.processing.ocr.pytesseract")
    def test_tesseract_confidence_failure_returns_zero(self, mock_pytesseract: MagicMock) -> None:
        """7.1.10: When image_to_data raises RuntimeError, confidence returns 0.0."""
        mock_pytesseract.image_to_string.return_value = "Some text"
        mock_pytesseract.image_to_data.side_effect = RuntimeError("data extraction failed")
        mock_pytesseract.Output.DICT = "dict"
        mock_pytesseract.TesseractNotFoundError = type("TesseractNotFoundError", (Exception,), {})
        mock_pytesseract.TesseractError = type("TesseractError", (Exception,), {})

        engine = TesseractEngine()
        result = engine.extract_text(_make_test_image())

        assert result.confidence == 0.0

    @patch("data_collector.processing.ocr.pytesseract")
    def test_tesseract_confidence_all_negative_one(self, mock_pytesseract: MagicMock) -> None:
        """7.1.11: When all confidence values are -1, confidence returns 0.0."""
        mock_pytesseract.image_to_string.return_value = "Text"
        mock_pytesseract.image_to_data.return_value = {"conf": [-1, -1, -1]}
        mock_pytesseract.Output.DICT = "dict"
        mock_pytesseract.TesseractNotFoundError = type("TesseractNotFoundError", (Exception,), {})
        mock_pytesseract.TesseractError = type("TesseractError", (Exception,), {})

        engine = TesseractEngine()
        result = engine.extract_text(_make_test_image())

        assert result.confidence == 0.0
