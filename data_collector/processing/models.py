"""Processing data models and exception types.

Provides immutable result types for PDF classification, text extraction,
table extraction, and OCR operations. Custom exception hierarchy for
processing-specific errors.
"""

from __future__ import annotations

from dataclasses import dataclass

from data_collector.enums.processing import PdfDataType


@dataclass(frozen=True)
class PDFClassification:
    """Immutable result from PDF document classification.

    Attributes:
        document_type: Whether the document is text-based or image-based.
        total_pages: Total number of pages in the PDF.
        text_pages: Number of pages with extractable text above the threshold.
        text_ratio: Fraction of pages classified as text (0.0-1.0).
    """

    document_type: PdfDataType
    total_pages: int
    text_pages: int
    text_ratio: float


@dataclass(frozen=True)
class PageText:
    """Extracted text from a single PDF page.

    Attributes:
        page_number: 1-based page number.
        text: Extracted text content (empty string if no text found).
    """

    page_number: int
    text: str


@dataclass(frozen=True)
class PDFText:
    """Immutable result from full PDF text extraction.

    Attributes:
        pages: Per-page extracted text.
        full_text: Concatenated text from all pages (joined by newlines).
    """

    pages: tuple[PageText, ...]
    full_text: str


@dataclass(frozen=True)
class PDFTable:
    """A single extracted table from a PDF page.

    Attributes:
        page_number: 1-based page number where the table was found.
        rows: Table data as a tuple of tuples (header row + data rows).
            Each inner tuple represents one row; cell values are strings or None.
    """

    page_number: int
    rows: tuple[tuple[str | None, ...], ...]


@dataclass(frozen=True)
class PDFTables:
    """Immutable result from PDF table extraction.

    Attributes:
        tables: All tables found across all pages.
        total_tables: Number of tables found.
    """

    tables: tuple[PDFTable, ...]
    total_tables: int


@dataclass(frozen=True)
class OCRResult:
    """Immutable result from OCR text extraction.

    Attributes:
        text: Extracted text from the image or PDF page.
        engine_name: Name of the OCR engine that produced this result.
        preprocessed: Whether image preprocessing was applied.
        confidence: Average confidence score (0.0-1.0) across all detected
            text lines or words. PaddleOCR provides per-line confidence;
            Tesseract provides per-word confidence via ``image_to_data()``.
    """

    text: str
    engine_name: str
    preprocessed: bool
    confidence: float


class ProcessingError(Exception):
    """Base exception for all processing-related errors.

    Attributes:
        message: Human-readable error description.
        source_path: Path to the file that caused the error, if available.
    """

    def __init__(self, message: str, source_path: str = "") -> None:
        self.message = message
        self.source_path = source_path
        super().__init__(f"Processing error: {message}" + (f" (file: {source_path})" if source_path else ""))


class PDFExtractionError(ProcessingError):
    """Raised when PDF text or table extraction fails."""


class OCRError(ProcessingError):
    """Raised when OCR text extraction fails."""


class PreprocessingError(ProcessingError):
    """Raised when image preprocessing fails."""


class TrainingError(ProcessingError):
    """Raised when OCR training dataset operations fail (export, config generation, evaluation)."""
