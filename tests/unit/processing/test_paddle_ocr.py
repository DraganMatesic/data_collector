"""Tests for PaddleOCREngine with mocked PaddleOCR library."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from PIL import Image

from data_collector.processing.models import OCRError, OCRResult
from data_collector.processing.ocr import PaddleOCREngine, extract_text_ocr


def _make_test_image() -> Image.Image:
    """Create a minimal 100x100 RGB PIL Image for testing."""
    return Image.fromarray(np.zeros((100, 100, 3), dtype=np.uint8))


def _make_paddle_result(lines: list[tuple[str, float]]) -> list[list[list[object]]]:
    """Build a PaddleOCR-shaped result from (text, confidence) pairs.

    PaddleOCR returns::

        [
            [  # page/image
                [[[x1,y1],[x2,y2],[x3,y3],[x4,y4]], ("text", confidence)],
                ...
            ]
        ]

    Args:
        lines: List of (text, confidence) tuples.

    Returns:
        Nested list matching PaddleOCR output structure.
    """
    dummy_bounding_box = [[0, 0], [100, 0], [100, 30], [0, 30]]
    page: list[list[object]] = []
    for text, confidence in lines:
        page.append([dummy_bounding_box, (text, confidence)])
    return [page]


class TestPaddleOCREngine:
    """Tests for PaddleOCREngine with mocked PaddleOCR."""

    @patch("data_collector.processing.ocr.PaddleOCR")
    def test_engine_name(self, mock_paddle_class: MagicMock) -> None:
        engine = PaddleOCREngine()
        assert engine.engine_name == "paddleocr"

    @patch("data_collector.processing.ocr.PaddleOCR")
    def test_engine_init_creates_paddleocr(self, mock_paddle_class: MagicMock) -> None:
        PaddleOCREngine()
        mock_paddle_class.assert_called_once_with(use_angle_cls=True, lang="en", show_log=False)

    @patch("data_collector.processing.ocr.PaddleOCR")
    def test_custom_language(self, mock_paddle_class: MagicMock) -> None:
        PaddleOCREngine(language="hr")
        mock_paddle_class.assert_called_once_with(use_angle_cls=True, lang="hr", show_log=False)

    @patch("data_collector.processing.ocr.PaddleOCR")
    def test_basic_extraction(self, mock_paddle_class: MagicMock) -> None:
        paddle_result = _make_paddle_result([("Hello World", 0.95), ("Second line", 0.88)])
        mock_ocr_instance = MagicMock()
        mock_ocr_instance.ocr.return_value = paddle_result
        mock_paddle_class.return_value = mock_ocr_instance

        engine = PaddleOCREngine()
        result = engine.extract_text(_make_test_image())

        assert result.text == "Hello World\nSecond line"
        assert isinstance(result, OCRResult)
        assert result.engine_name == "paddleocr"

    @patch("data_collector.processing.ocr.PaddleOCR")
    def test_empty_result_none(self, mock_paddle_class: MagicMock) -> None:
        mock_ocr_instance = MagicMock()
        mock_ocr_instance.ocr.return_value = [None]
        mock_paddle_class.return_value = mock_ocr_instance

        engine = PaddleOCREngine()
        result = engine.extract_text(_make_test_image())

        assert result.text == ""

    @patch("data_collector.processing.ocr.PaddleOCR")
    def test_empty_result_empty_list(self, mock_paddle_class: MagicMock) -> None:
        mock_ocr_instance = MagicMock()
        mock_ocr_instance.ocr.return_value = [[]]
        mock_paddle_class.return_value = mock_ocr_instance

        engine = PaddleOCREngine()
        result = engine.extract_text(_make_test_image())

        assert result.text == ""

    @patch("data_collector.processing.ocr.PaddleOCR")
    def test_confidence_average(self, mock_paddle_class: MagicMock) -> None:
        paddle_result = _make_paddle_result([("Hello World", 0.95), ("Second line", 0.88)])
        mock_ocr_instance = MagicMock()
        mock_ocr_instance.ocr.return_value = paddle_result
        mock_paddle_class.return_value = mock_ocr_instance

        engine = PaddleOCREngine()
        result = engine.extract_text(_make_test_image())

        expected_confidence = (0.95 + 0.88) / 2
        assert result.confidence == pytest.approx(expected_confidence)

    @patch("data_collector.processing.ocr.PaddleOCR")
    def test_confidence_empty_result(self, mock_paddle_class: MagicMock) -> None:
        mock_ocr_instance = MagicMock()
        mock_ocr_instance.ocr.return_value = [None]
        mock_paddle_class.return_value = mock_ocr_instance

        engine = PaddleOCREngine()
        result = engine.extract_text(_make_test_image())

        assert result.confidence == 0.0

    @patch("data_collector.processing.ocr.PaddleOCR")
    def test_extraction_error_wrapped(self, mock_paddle_class: MagicMock) -> None:
        mock_ocr_instance = MagicMock()
        mock_ocr_instance.ocr.side_effect = RuntimeError("PaddleOCR internal failure")
        mock_paddle_class.return_value = mock_ocr_instance

        engine = PaddleOCREngine()
        with pytest.raises(OCRError, match="PaddleOCR extraction failed"):
            engine.extract_text(_make_test_image())

    @patch("data_collector.processing.ocr.PaddleOCREngine")
    def test_default_in_extract_text_ocr(self, mock_paddle_engine_class: MagicMock) -> None:
        mock_engine_instance = MagicMock()
        mock_engine_instance.engine_name = "paddleocr"
        mock_engine_instance.extract_text.return_value = OCRResult(
            text="default engine output", engine_name="paddleocr", preprocessed=False, confidence=0.92
        )
        mock_paddle_engine_class.return_value = mock_engine_instance

        result = extract_text_ocr(_make_test_image())

        mock_paddle_engine_class.assert_called_once()
        assert result.engine_name == "paddleocr"

    @patch("data_collector.processing.ocr.PaddleOCR")
    def test_empty_result_bare_none(self, mock_paddle_class: MagicMock) -> None:
        """7.4.2: When ocr.ocr() returns None (not [None]), text is empty and confidence is 0.0."""
        mock_ocr_instance = MagicMock()
        mock_ocr_instance.ocr.return_value = None
        mock_paddle_class.return_value = mock_ocr_instance

        engine = PaddleOCREngine()
        result = engine.extract_text(_make_test_image())

        assert result.text == ""
        assert result.confidence == 0.0
