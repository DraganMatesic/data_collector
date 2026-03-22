"""Tests for processing data models, enums, and exception types."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from data_collector.enums.processing import PdfDataType
from data_collector.processing.models import (
    OCRError,
    OCRResult,
    PageText,
    PDFClassification,
    PDFExtractionError,
    PDFTable,
    PDFTables,
    PDFText,
    PreprocessingError,
    ProcessingError,
)


class TestPdfDataType:
    """Tests for PdfDataType StrEnum values."""

    def test_text_value(self) -> None:
        assert PdfDataType.TEXT == "text"

    def test_image_value(self) -> None:
        assert PdfDataType.IMAGE == "image"

    def test_member_count(self) -> None:
        assert len(PdfDataType) == 2

    def test_is_str(self) -> None:
        assert isinstance(PdfDataType.TEXT, str)
        assert isinstance(PdfDataType.IMAGE, str)


class TestPDFClassification:
    """Tests for PDFClassification frozen dataclass."""

    def test_construction(self) -> None:
        classification = PDFClassification(
            document_type=PdfDataType.TEXT,
            total_pages=10,
            text_pages=9,
            text_ratio=0.9,
        )
        assert classification.document_type == PdfDataType.TEXT
        assert classification.total_pages == 10
        assert classification.text_pages == 9
        assert classification.text_ratio == 0.9

    def test_frozen(self) -> None:
        classification = PDFClassification(
            document_type=PdfDataType.IMAGE,
            total_pages=5,
            text_pages=1,
            text_ratio=0.2,
        )
        with pytest.raises(FrozenInstanceError):
            classification.total_pages = 20  # type: ignore[misc]

    def test_all_attributes_accessible(self) -> None:
        classification = PDFClassification(
            document_type=PdfDataType.TEXT,
            total_pages=1,
            text_pages=1,
            text_ratio=1.0,
        )
        assert hasattr(classification, "document_type")
        assert hasattr(classification, "total_pages")
        assert hasattr(classification, "text_pages")
        assert hasattr(classification, "text_ratio")


class TestPageText:
    """Tests for PageText frozen dataclass."""

    def test_construction(self) -> None:
        page = PageText(page_number=1, text="Hello world")
        assert page.page_number == 1
        assert page.text == "Hello world"

    def test_frozen(self) -> None:
        page = PageText(page_number=1, text="content")
        with pytest.raises(FrozenInstanceError):
            page.text = "modified"  # type: ignore[misc]


class TestPDFText:
    """Tests for PDFText frozen dataclass."""

    def test_construction_with_pages(self) -> None:
        page_one = PageText(page_number=1, text="First page")
        page_two = PageText(page_number=2, text="Second page")
        pdf_text = PDFText(
            pages=(page_one, page_two),
            full_text="First page\nSecond page",
        )
        assert len(pdf_text.pages) == 2
        assert pdf_text.pages[0].text == "First page"
        assert pdf_text.pages[1].text == "Second page"

    def test_full_text_accessible(self) -> None:
        page = PageText(page_number=1, text="Only page")
        pdf_text = PDFText(pages=(page,), full_text="Only page")
        assert pdf_text.full_text == "Only page"


class TestPDFTable:
    """Tests for PDFTable frozen dataclass."""

    def test_construction_with_none_cells(self) -> None:
        table = PDFTable(
            page_number=3,
            rows=(
                ("Header A", "Header B", None),
                ("cell 1", None, "cell 3"),
            ),
        )
        assert table.page_number == 3
        assert table.rows[0] == ("Header A", "Header B", None)
        assert table.rows[1][1] is None

    def test_frozen(self) -> None:
        table = PDFTable(page_number=1, rows=(("a", "b"),))
        with pytest.raises(FrozenInstanceError):
            table.page_number = 2  # type: ignore[misc]


class TestPDFTables:
    """Tests for PDFTables frozen dataclass."""

    def test_construction(self) -> None:
        table_one = PDFTable(page_number=1, rows=(("x", "y"),))
        table_two = PDFTable(page_number=2, rows=(("a", "b"),))
        pdf_tables = PDFTables(tables=(table_one, table_two), total_tables=2)
        assert len(pdf_tables.tables) == 2
        assert pdf_tables.total_tables == 2

    def test_total_tables_attribute(self) -> None:
        pdf_tables = PDFTables(tables=(), total_tables=0)
        assert pdf_tables.total_tables == 0


class TestOCRResult:
    """Tests for OCRResult frozen dataclass."""

    def test_construction_with_confidence(self) -> None:
        result = OCRResult(
            text="Extracted text",
            engine_name="tesseract",
            preprocessed=True,
            confidence=0.95,
        )
        assert result.text == "Extracted text"
        assert result.engine_name == "tesseract"
        assert result.preprocessed is True
        assert result.confidence == 0.95

    def test_frozen(self) -> None:
        result = OCRResult(text="abc", engine_name="paddle", preprocessed=False, confidence=0.8)
        with pytest.raises(FrozenInstanceError):
            result.confidence = 0.5  # type: ignore[misc]


class TestProcessingError:
    """Tests for ProcessingError base exception."""

    def test_message_attribute(self) -> None:
        error = ProcessingError("Something failed")
        assert error.message == "Something failed"

    def test_source_path_defaults_to_empty(self) -> None:
        error = ProcessingError("fail")
        assert error.source_path == ""

    def test_str_format_without_source_path(self) -> None:
        error = ProcessingError("extraction failed")
        assert "Processing error:" in str(error)
        assert "extraction failed" in str(error)

    def test_str_format_with_source_path(self) -> None:
        error = ProcessingError("parse error", source_path="/tmp/doc.pdf")
        error_string = str(error)
        assert "Processing error:" in error_string
        assert "(file: /tmp/doc.pdf)" in error_string

    def test_is_exception(self) -> None:
        assert isinstance(ProcessingError("msg"), Exception)

    def test_can_be_raised_and_caught(self) -> None:
        with pytest.raises(ProcessingError, match="Processing error"):
            raise ProcessingError("broken")


class TestPDFExtractionError:
    """Tests for PDFExtractionError exception."""

    def test_isinstance_processing_error(self) -> None:
        error = PDFExtractionError("table extraction failed")
        assert isinstance(error, ProcessingError)

    def test_inherits_message_format(self) -> None:
        error = PDFExtractionError("bad table", source_path="/data/file.pdf")
        assert "Processing error:" in str(error)
        assert "(file: /data/file.pdf)" in str(error)


class TestOCRError:
    """Tests for OCRError exception."""

    def test_isinstance_processing_error(self) -> None:
        error = OCRError("OCR engine crashed")
        assert isinstance(error, ProcessingError)

    def test_inherits_message_format(self) -> None:
        error = OCRError("engine timeout", source_path="/images/scan.png")
        assert "Processing error:" in str(error)
        assert "(file: /images/scan.png)" in str(error)


class TestPreprocessingError:
    """Tests for PreprocessingError exception."""

    def test_isinstance_processing_error(self) -> None:
        error = PreprocessingError("deskew failed")
        assert isinstance(error, ProcessingError)

    def test_inherits_message_format(self) -> None:
        error = PreprocessingError("grayscale conversion failed", source_path="/tmp/image.tiff")
        assert "Processing error:" in str(error)
        assert "(file: /tmp/image.tiff)" in str(error)
