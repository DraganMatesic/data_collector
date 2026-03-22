"""Unit tests for PDF classification, text extraction, and table extraction."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from data_collector.enums.processing import PdfDataType
from data_collector.processing.models import PDFExtractionError
from data_collector.processing.pdf import classify_pdf, extract_tables_pdfplumber, extract_text_pdfplumber

MODULE = "data_collector.processing.pdf"


def _make_mock_pdf(
    pages_text: list[str | None],
    pages_tables: list[list[list[list[Any]]]] | None = None,
) -> MagicMock:
    """Create a mock pdfplumber PDF context manager.

    Args:
        pages_text: Text returned by each page's extract_text(). None means no text.
        pages_tables: Per-page list of tables. Each table is a list of rows (list of cells).
    """
    mock_pdf = MagicMock()
    mock_pages: list[MagicMock] = []
    for index, text in enumerate(pages_text):
        page = MagicMock()
        page.extract_text.return_value = text
        tables = pages_tables[index] if pages_tables and index < len(pages_tables) else []
        page.extract_tables.return_value = tables
        mock_pages.append(page)
    mock_pdf.pages = mock_pages
    mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
    mock_pdf.__exit__ = MagicMock(return_value=False)
    return mock_pdf


def _long_text(length: int = 60) -> str:
    """Return a string of the given length for page text simulation."""
    return "A" * length


class TestClassifyPdf:
    """Tests for the classify_pdf function."""

    @patch(f"{MODULE}.pdfplumber")
    def test_all_text_pages(self, mock_pdfplumber: MagicMock) -> None:
        """10 pages all with sufficient text -> PdfDataType.TEXT."""
        mock_pdfplumber.open.return_value = _make_mock_pdf([_long_text()] * 10)

        result = classify_pdf("test.pdf")

        assert result.document_type == PdfDataType.TEXT
        assert result.total_pages == 10
        assert result.text_pages == 10
        assert result.text_ratio == 1.0

    @patch(f"{MODULE}.pdfplumber")
    def test_all_image_pages(self, mock_pdfplumber: MagicMock) -> None:
        """10 pages all with None/empty text -> PdfDataType.IMAGE."""
        mock_pdfplumber.open.return_value = _make_mock_pdf([None] * 10)

        result = classify_pdf("test.pdf")

        assert result.document_type == PdfDataType.IMAGE
        assert result.total_pages == 10
        assert result.text_pages == 0
        assert result.text_ratio == 0.0

    @patch(f"{MODULE}.pdfplumber")
    def test_threshold_boundary_exactly_90_percent(self, mock_pdfplumber: MagicMock) -> None:
        """9 of 10 pages have text (0.9 >= 0.9 threshold) -> TEXT."""
        pages = [_long_text()] * 9 + [None]
        mock_pdfplumber.open.return_value = _make_mock_pdf(pages)

        result = classify_pdf("test.pdf")

        assert result.document_type == PdfDataType.TEXT
        assert result.text_ratio == 0.9

    @patch(f"{MODULE}.pdfplumber")
    def test_threshold_boundary_below(self, mock_pdfplumber: MagicMock) -> None:
        """8 of 10 pages have text (0.8 < 0.9 threshold) -> IMAGE."""
        pages = [_long_text()] * 8 + [None, None]
        mock_pdfplumber.open.return_value = _make_mock_pdf(pages)

        result = classify_pdf("test.pdf")

        assert result.document_type == PdfDataType.IMAGE
        assert result.text_ratio == 0.8

    @patch(f"{MODULE}.pdfplumber")
    def test_empty_pdf_zero_pages(self, mock_pdfplumber: MagicMock) -> None:
        """PDF with zero pages -> IMAGE with 0.0 ratio."""
        mock_pdfplumber.open.return_value = _make_mock_pdf([])

        result = classify_pdf("test.pdf")

        assert result.document_type == PdfDataType.IMAGE
        assert result.total_pages == 0
        assert result.text_pages == 0
        assert result.text_ratio == 0.0

    @patch(f"{MODULE}.pdfplumber")
    def test_single_page_with_text(self, mock_pdfplumber: MagicMock) -> None:
        """Single page with text -> TEXT (1/1 = 1.0 >= 0.9)."""
        mock_pdfplumber.open.return_value = _make_mock_pdf([_long_text()])

        result = classify_pdf("test.pdf")

        assert result.document_type == PdfDataType.TEXT
        assert result.total_pages == 1
        assert result.text_pages == 1

    @patch(f"{MODULE}.pdfplumber")
    def test_min_text_length_filtering(self, mock_pdfplumber: MagicMock) -> None:
        """Page with short text (<50 chars) is treated as no text."""
        short_text = "A" * 30
        mock_pdfplumber.open.return_value = _make_mock_pdf([short_text])

        result = classify_pdf("test.pdf", min_text_length=50)

        assert result.document_type == PdfDataType.IMAGE
        assert result.text_pages == 0

    @patch(f"{MODULE}.pdfplumber")
    def test_custom_threshold(self, mock_pdfplumber: MagicMock) -> None:
        """With threshold=0.5, 5/10 pages with text -> TEXT."""
        pages = [_long_text()] * 5 + [None] * 5
        mock_pdfplumber.open.return_value = _make_mock_pdf(pages)

        result = classify_pdf("test.pdf", threshold=0.5)

        assert result.document_type == PdfDataType.TEXT
        assert result.text_ratio == 0.5

    @patch(f"{MODULE}.pdfplumber")
    def test_pdfplumber_error_wrapped(self, mock_pdfplumber: MagicMock) -> None:
        """Exception from pdfplumber.open is wrapped in PDFExtractionError."""
        mock_pdfplumber.open.side_effect = RuntimeError("corrupt file")

        with pytest.raises(PDFExtractionError, match="Failed to classify PDF"):
            classify_pdf("bad.pdf")


class TestExtractTextPdfplumber:
    """Tests for the extract_text_pdfplumber function."""

    @patch(f"{MODULE}.pdfplumber")
    def test_all_pages(self, mock_pdfplumber: MagicMock) -> None:
        """Extract all pages and verify page count and full_text."""
        mock_pdfplumber.open.return_value = _make_mock_pdf(["Page one.", "Page two.", "Page three."])

        result = extract_text_pdfplumber("test.pdf")

        assert len(result.pages) == 3
        assert result.pages[0].page_number == 1
        assert result.pages[0].text == "Page one."
        assert result.pages[2].text == "Page three."
        assert "Page one." in result.full_text
        assert "Page three." in result.full_text

    @patch(f"{MODULE}.pdfplumber")
    def test_specific_page_numbers(self, mock_pdfplumber: MagicMock) -> None:
        """Extract only pages 1 and 3 from a 3-page PDF."""
        mock_pdfplumber.open.return_value = _make_mock_pdf(["First", "Second", "Third"])

        result = extract_text_pdfplumber("test.pdf", page_numbers=[1, 3])

        assert len(result.pages) == 2
        assert result.pages[0].page_number == 1
        assert result.pages[0].text == "First"
        assert result.pages[1].page_number == 3
        assert result.pages[1].text == "Third"

    @patch(f"{MODULE}.pdfplumber")
    def test_page_with_no_text(self, mock_pdfplumber: MagicMock) -> None:
        """Page returning None -> empty string in PageText."""
        mock_pdfplumber.open.return_value = _make_mock_pdf([None])

        result = extract_text_pdfplumber("test.pdf")

        assert len(result.pages) == 1
        assert result.pages[0].text == ""

    @patch(f"{MODULE}.pdfplumber")
    def test_full_text_joined_by_newline(self, mock_pdfplumber: MagicMock) -> None:
        """Verify full_text is pages joined by newline."""
        mock_pdfplumber.open.return_value = _make_mock_pdf(["Alpha", "Beta"])

        result = extract_text_pdfplumber("test.pdf")

        assert result.full_text == "Alpha\nBeta"

    @patch(f"{MODULE}.pdfplumber")
    def test_invalid_page_number_zero(self, mock_pdfplumber: MagicMock) -> None:
        """Page number 0 is out of range -> PDFExtractionError."""
        mock_pdfplumber.open.return_value = _make_mock_pdf(["Some text."])

        with pytest.raises(PDFExtractionError, match="out of range"):
            extract_text_pdfplumber("test.pdf", page_numbers=[0])

    @patch(f"{MODULE}.pdfplumber")
    def test_invalid_page_number_beyond_total(self, mock_pdfplumber: MagicMock) -> None:
        """Page number beyond total pages -> PDFExtractionError."""
        mock_pdfplumber.open.return_value = _make_mock_pdf(["Some text."])

        with pytest.raises(PDFExtractionError, match="out of range"):
            extract_text_pdfplumber("test.pdf", page_numbers=[999])


class TestExtractTablesPdfplumber:
    """Tests for the extract_tables_pdfplumber function."""

    @patch(f"{MODULE}.pdfplumber")
    def test_pages_with_tables(self, mock_pdfplumber: MagicMock) -> None:
        """Verify PDFTables contains extracted tables with correct structure."""
        table_data = [[["Name", "Age"], ["Alice", "30"]]]
        mock_pdfplumber.open.return_value = _make_mock_pdf(
            pages_text=["text"],
            pages_tables=[table_data],
        )

        result = extract_tables_pdfplumber("test.pdf")

        assert result.total_tables == 1
        assert len(result.tables) == 1
        assert result.tables[0].page_number == 1
        assert result.tables[0].rows == (("Name", "Age"), ("Alice", "30"))

    @patch(f"{MODULE}.pdfplumber")
    def test_no_tables(self, mock_pdfplumber: MagicMock) -> None:
        """Pages with no tables -> PDFTables with empty tuple."""
        mock_pdfplumber.open.return_value = _make_mock_pdf(
            pages_text=["text", "more text"],
            pages_tables=[[], []],
        )

        result = extract_tables_pdfplumber("test.pdf")

        assert result.total_tables == 0
        assert result.tables == ()

    @patch(f"{MODULE}.pdfplumber")
    def test_none_cells_in_table(self, mock_pdfplumber: MagicMock) -> None:
        """Table with None cell values preserved in output."""
        table_with_nones = [[["Header", None], [None, "Value"]]]
        mock_pdfplumber.open.return_value = _make_mock_pdf(
            pages_text=["text"],
            pages_tables=[table_with_nones],
        )

        result = extract_tables_pdfplumber("test.pdf")

        assert result.total_tables == 1
        assert result.tables[0].rows == (("Header", None), (None, "Value"))

    @patch(f"{MODULE}.pdfplumber")
    def test_custom_table_settings(self, mock_pdfplumber: MagicMock) -> None:
        """Verify table_settings dict is passed through to extract_tables()."""
        mock_pdf = _make_mock_pdf(pages_text=["text"], pages_tables=[[]])
        mock_pdfplumber.open.return_value = mock_pdf

        custom_settings: dict[str, object] = {"vertical_strategy": "text", "horizontal_strategy": "text"}
        extract_tables_pdfplumber("test.pdf", table_settings=custom_settings)

        mock_pdf.pages[0].extract_tables.assert_called_once_with(custom_settings)

    @patch(f"{MODULE}.pdfplumber")
    def test_extract_tables_invalid_page_number_zero(self, mock_pdfplumber: MagicMock) -> None:
        """7.2.2: Page number 0 for extract_tables raises PDFExtractionError."""
        mock_pdfplumber.open.return_value = _make_mock_pdf(
            pages_text=["text"],
            pages_tables=[[[["A", "B"]]]],
        )

        with pytest.raises(PDFExtractionError, match="out of range"):
            extract_tables_pdfplumber("test.pdf", page_numbers=[0])


class TestExtractTextZeroPages:
    """Tests for edge cases with zero-page PDFs."""

    @patch(f"{MODULE}.pdfplumber")
    def test_extract_text_zero_pages(self, mock_pdfplumber: MagicMock) -> None:
        """7.1.7: PDF with 0 pages returns PDFText with empty pages tuple and empty full_text."""
        mock_pdfplumber.open.return_value = _make_mock_pdf([])

        result = extract_text_pdfplumber("empty.pdf")

        assert result.pages == ()
        assert result.full_text == ""

    @patch(f"{MODULE}.pdfplumber")
    def test_extract_tables_zero_pages(self, mock_pdfplumber: MagicMock) -> None:
        """7.1.7: PDF with 0 pages returns PDFTables with empty tables and zero total."""
        mock_pdfplumber.open.return_value = _make_mock_pdf([])

        result = extract_tables_pdfplumber("empty.pdf")

        assert result.tables == ()
        assert result.total_tables == 0


class TestPdfplumberErrorWrapping:
    """Tests for error wrapping in extraction functions."""

    @patch(f"{MODULE}.pdfplumber")
    def test_extract_text_pdfplumber_error_wrapped(self, mock_pdfplumber: MagicMock) -> None:
        """7.1.8: Exception from pdfplumber.open in extract_text is wrapped in PDFExtractionError."""
        mock_pdfplumber.open.side_effect = RuntimeError("corrupt PDF data")

        with pytest.raises(PDFExtractionError, match="Failed to extract text from PDF"):
            extract_text_pdfplumber("corrupt.pdf")

    @patch(f"{MODULE}.pdfplumber")
    def test_extract_tables_pdfplumber_error_wrapped(self, mock_pdfplumber: MagicMock) -> None:
        """7.1.8: Exception from pdfplumber.open in extract_tables is wrapped in PDFExtractionError."""
        mock_pdfplumber.open.side_effect = RuntimeError("corrupt PDF data")

        with pytest.raises(PDFExtractionError, match="Failed to extract tables from PDF"):
            extract_tables_pdfplumber("corrupt.pdf")

    @patch(f"{MODULE}.pdfplumber")
    def test_invalid_negative_page_number(self, mock_pdfplumber: MagicMock) -> None:
        """7.1.9: Negative page number raises PDFExtractionError."""
        mock_pdfplumber.open.return_value = _make_mock_pdf(["Some text."])

        with pytest.raises(PDFExtractionError, match="out of range"):
            extract_text_pdfplumber("test.pdf", page_numbers=[-1])
