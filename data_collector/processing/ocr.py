"""OCR engine adapter pattern with PaddleOCR as primary and Tesseract as fallback.

Provides an abstract OCREngine interface with two concrete implementations:
PaddleOCREngine (deep learning, high accuracy) and TesseractEngine (CLI-based,
lightweight fallback). A convenience function ``extract_text_ocr`` handles file
opening and engine selection.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import numpy as np
import pytesseract
from paddleocr import PaddleOCR
from PIL import Image

from data_collector.processing.models import OCRError, OCRResult
from data_collector.processing.preprocessing import ImagePreprocessor

logger = logging.getLogger(__name__)


class OCREngine(ABC):
    """Abstract OCR engine interface.

    All OCR engines accept a PIL Image and return extracted text with
    confidence. Implementations must define engine_name, _extract, and
    _confidence.

    Args:
        preprocessor: Optional ImagePreprocessor. When provided, images
            are preprocessed before OCR.
    """

    def __init__(self, *, preprocessor: ImagePreprocessor | None = None) -> None:
        self._preprocessor = preprocessor

    @property
    @abstractmethod
    def engine_name(self) -> str:
        """Unique identifier for this OCR engine."""
        ...

    @abstractmethod
    def _extract(self, image: Image.Image) -> str:
        """Engine-specific text extraction.

        Args:
            image: PIL Image to extract text from.

        Returns:
            Extracted text as a single string.
        """
        ...

    @abstractmethod
    def _confidence(self, image: Image.Image) -> float:
        """Engine-specific confidence score.

        Args:
            image: PIL Image that was processed (same image passed to _extract).

        Returns:
            Confidence score between 0.0 and 1.0.
        """
        ...

    def extract_text(self, image: Image.Image, *, dpi: int | None = None) -> OCRResult:
        """Extract text with optional preprocessing.

        Runs the preprocessor if configured, then calls _extract() followed by
        _confidence() on the same (possibly preprocessed) image.

        Args:
            image: PIL Image to process.
            dpi: Image DPI hint passed to the preprocessor for upscale decisions.

        Returns:
            OCRResult with extracted text, engine name, preprocessing flag, and confidence.
        """
        preprocessed = False
        if self._preprocessor is not None:
            image = self._preprocessor.run(image, dpi=dpi)
            preprocessed = True

        text = self._extract(image)
        confidence = self._confidence(image)
        return OCRResult(text=text, engine_name=self.engine_name, preprocessed=preprocessed, confidence=confidence)


class PaddleOCREngine(OCREngine):
    """PaddleOCR engine -- primary OCR engine for the framework.

    Uses PP-OCR deep learning models for text detection and recognition.
    Supports 100+ languages. Superior accuracy (F1=0.938) compared to
    Tesseract, especially on complex layouts and mixed-language documents.

    Each instance creates a PaddleOCR object in ``__init__`` (loads 3 neural
    networks, ~2-5s). The instance is reused across all ``extract_text()``
    calls. Designed for Dramatiq worker processes where each worker creates
    its own engine instance.

    Args:
        language: PaddleOCR language code (e.g., "en", "hr", "ch").
        use_angle_classifier: Enable text angle classification (0/180 degree).
        preprocessor: Optional ImagePreprocessor.
    """

    def __init__(
        self,
        *,
        language: str = "en",
        use_angle_classifier: bool = True,
        preprocessor: ImagePreprocessor | None = None,
    ) -> None:
        super().__init__(preprocessor=preprocessor)
        self._language = language
        self._ocr = PaddleOCR(use_angle_cls=use_angle_classifier, lang=language, show_log=False)
        self._last_result: list[Any] | None = None

    @property
    def engine_name(self) -> str:
        """Returns the engine identifier."""
        return "paddleocr"

    def _extract(self, image: Image.Image) -> str:
        """Extract text using PaddleOCR.

        Converts the PIL Image to a numpy array, runs OCR, and joins detected
        text lines with newlines. Caches the raw result for ``_confidence()``
        to avoid running OCR twice.

        Args:
            image: PIL Image to extract text from.

        Returns:
            Extracted text with lines separated by newlines.

        Raises:
            OCRError: If PaddleOCR fails during extraction.
        """
        self._last_result = None
        try:
            image_array = np.array(image)
            result = self._ocr.ocr(image_array, cls=True)
            self._last_result = result

            if not result or not result[0]:
                return ""

            lines: list[str] = []
            for line in result[0]:
                text = line[1][0]
                lines.append(text)
            return "\n".join(lines)
        except OCRError:
            raise
        except Exception as error:
            raise OCRError(f"PaddleOCR extraction failed: {error}") from error

    def _confidence(self, image: Image.Image) -> float:
        """Average per-line confidence from cached PaddleOCR result.

        Uses the result cached by ``_extract()`` to avoid a redundant OCR pass.
        Must be called after ``_extract()`` on the same image.

        Args:
            image: PIL Image (unused; confidence is derived from cached result).

        Returns:
            Average confidence between 0.0 and 1.0, or 0.0 if no text was detected.
        """
        if not self._last_result or not self._last_result[0]:
            return 0.0

        confidences: list[float] = [line[1][1] for line in self._last_result[0]]
        if not confidences:
            return 0.0
        return sum(confidences) / len(confidences)


class TesseractEngine(OCREngine):
    """Tesseract OCR engine -- fallback for local dev and testing.

    Requires the Tesseract binary installed on the system. pytesseract
    is a thin wrapper that calls the Tesseract CLI.

    Lighter initialization than PaddleOCR but lower accuracy (F1=0.797).
    Works well with ``ThreadPoolExecutor`` (each call spawns a subprocess).

    Prerequisites:
        - Install Tesseract: https://github.com/tesseract-ocr/tesseract
        - Windows: installer adds to PATH, or set ``tesseract_cmd``.
        - Linux: ``apt install tesseract-ocr``

    Args:
        language: Tesseract language string (e.g., "eng", "eng+hrv").
        tesseract_cmd: Path to the Tesseract executable. Empty string uses PATH lookup.
        preprocessor: Optional ImagePreprocessor.
    """

    def __init__(
        self,
        *,
        language: str = "eng",
        tesseract_cmd: str = "",
        preprocessor: ImagePreprocessor | None = None,
    ) -> None:
        super().__init__(preprocessor=preprocessor)
        self._language = language
        if tesseract_cmd:
            # pytesseract uses a module-level global for the binary path. If multiple
            # TesseractEngine instances with different tesseract_cmd values exist in the
            # same process, the last one wins.
            pytesseract.pytesseract.tesseract_cmd = tesseract_cmd

    @property
    def engine_name(self) -> str:
        """Returns the engine identifier."""
        return "tesseract"

    def _extract(self, image: Image.Image) -> str:
        """Extract text using Tesseract via pytesseract.

        Args:
            image: PIL Image to extract text from.

        Returns:
            Extracted text with leading/trailing whitespace stripped.

        Raises:
            OCRError: If the Tesseract binary is not found or extraction fails.
        """
        try:
            text: str = pytesseract.image_to_string(image, lang=self._language)
            return text.strip()
        except pytesseract.TesseractNotFoundError as error:
            raise OCRError(
                "Tesseract binary not found. Install Tesseract and ensure it is on PATH, "
                "or set DC_PROCESSING_TESSERACT_CMD."
            ) from error
        except pytesseract.TesseractError as error:
            raise OCRError(f"Tesseract extraction failed: {error}") from error

    def _confidence(self, image: Image.Image) -> float:
        """Per-word confidence from Tesseract ``image_to_data()``.

        Runs ``image_to_data()`` on the image and averages all per-word
        confidence values, excluding entries with confidence -1 (non-text
        regions). Tesseract returns confidence on a 0-100 scale; this method
        normalizes to 0.0-1.0.

        Args:
            image: PIL Image to compute confidence for.

        Returns:
            Average confidence between 0.0 and 1.0, or 0.0 if no words were detected
            or confidence computation fails.
        """
        try:
            data = pytesseract.image_to_data(image, lang=self._language, output_type=pytesseract.Output.DICT)
            confidences = [int(confidence) for confidence in data["conf"] if int(confidence) >= 0]
            if not confidences:
                return 0.0
            return sum(confidences) / (len(confidences) * 100.0)
        except Exception as error:
            logger.warning(f"Failed to compute Tesseract confidence: {type(error).__name__}: {error}")
            return 0.0


def extract_text_ocr(
    source: str | Path | Image.Image,
    *,
    engine: OCREngine | None = None,
    dpi: int | None = None,
) -> OCRResult:
    """Extract text from an image or image-based PDF page using OCR.

    Convenience function that opens the source, applies the OCR engine,
    and returns structured results.

    When source is a file path, opens it as a PIL Image. For multi-page
    PDFs, convert pages to images externally and call this per page.

    Args:
        source: File path (str or Path) to an image, or a PIL Image object.
        engine: OCR engine instance. Defaults to PaddleOCREngine().
        dpi: Image DPI for preprocessing upscale decisions.

    Returns:
        OCRResult with extracted text, engine name, and confidence.

    Raises:
        OCRError: If the source cannot be opened or OCR fails.
    """
    if engine is None:
        engine = PaddleOCREngine()

    if isinstance(source, (str, Path)):
        source_path = Path(source)
        try:
            with Image.open(source_path) as image:
                logger.info(f"Running OCR with {engine.engine_name} engine")
                return engine.extract_text(image, dpi=dpi)
        except OCRError:
            raise
        except Exception as error:
            raise OCRError(f"Cannot open image: {error}", source_path=str(source_path)) from error
    else:
        image = source

    logger.info(f"Running OCR with {engine.engine_name} engine")
    return engine.extract_text(image, dpi=dpi)
