"""PDF text and table extraction via pdfplumber.

Provides classification of PDF documents as text-based or image-based,
full-text extraction, and table extraction using pdfplumber as the
underlying engine.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pdfplumber

from data_collector.enums.processing import PdfDataType
from data_collector.processing.models import (
    PageText,
    PDFClassification,
    PDFExtractionError,
    PDFTable,
    PDFTables,
    PDFText,
)

logger = logging.getLogger(__name__)


def classify_pdf(
    pdf_path: str | Path,
    *,
    threshold: float = 0.9,
    min_text_length: int = 50,
) -> PDFClassification:
    """Classify a PDF as text-based or image-based.

    Opens the PDF and examines each page for extractable text. If the
    fraction of pages with meaningful text (>= min_text_length characters)
    meets or exceeds the threshold, the document is classified as TEXT.

    Args:
        pdf_path: Path to the PDF file.
        threshold: Minimum fraction of text pages required for TEXT classification (0.0-1.0).
        min_text_length: Minimum stripped text length for a page to count as a text page.

    Returns:
        PDFClassification with document type, page counts, and text ratio.

    Raises:
        PDFExtractionError: If the PDF cannot be opened or read.
    """
    source_path = str(pdf_path)
    try:
        with pdfplumber.open(source_path) as pdf:
            total_pages = len(pdf.pages)

            if total_pages == 0:
                classification = PDFClassification(
                    document_type=PdfDataType.IMAGE,
                    total_pages=0,
                    text_pages=0,
                    text_ratio=0.0,
                )
                logger.info(f"Classified {pdf_path}: {classification.document_type} (0 pages)")
                return classification

            text_page_count = 0
            for page in pdf.pages:
                extracted = page.extract_text()
                if extracted and len(extracted.strip()) >= min_text_length:
                    text_page_count += 1

            text_ratio = text_page_count / total_pages
            document_type = PdfDataType.TEXT if text_ratio >= threshold else PdfDataType.IMAGE

            classification = PDFClassification(
                document_type=document_type,
                total_pages=total_pages,
                text_pages=text_page_count,
                text_ratio=text_ratio,
            )
            logger.info(
                f"Classified {pdf_path}: {classification.document_type}"
                f" ({text_page_count}/{total_pages} text pages, ratio={text_ratio:.2f})"
            )
            return classification

    except PDFExtractionError:
        raise
    except Exception as error:
        raise PDFExtractionError(f"Failed to classify PDF: {error}", source_path=source_path) from error


def extract_text_pdfplumber(
    pdf_path: str | Path,
    *,
    page_numbers: list[int] | None = None,
) -> PDFText:
    """Extract text from a PDF using pdfplumber.

    Args:
        pdf_path: Path to the PDF file.
        page_numbers: 1-based page numbers to extract. When None, all pages are extracted.

    Returns:
        PDFText containing per-page text and concatenated full text.

    Raises:
        PDFExtractionError: If the PDF cannot be opened or page numbers are invalid.
    """
    source_path = str(pdf_path)
    try:
        with pdfplumber.open(source_path) as pdf:
            total_pages = len(pdf.pages)

            if page_numbers is not None:
                _validate_page_numbers(page_numbers, total_pages, source_path)
                selected_indices = [page_number - 1 for page_number in page_numbers]
            else:
                selected_indices = list(range(total_pages))

            page_texts: list[PageText] = []
            for index in selected_indices:
                page = pdf.pages[index]
                extracted = page.extract_text()
                text = extracted if extracted else ""
                page_texts.append(PageText(page_number=index + 1, text=text))

            full_text = "\n".join(page_text.text for page_text in page_texts)

            logger.info(f"Extracted text from {pdf_path}: {len(page_texts)} pages, {len(full_text)} characters")

            return PDFText(pages=tuple(page_texts), full_text=full_text)

    except PDFExtractionError:
        raise
    except Exception as error:
        raise PDFExtractionError(f"Failed to extract text from PDF: {error}", source_path=source_path) from error


def extract_tables_pdfplumber(
    pdf_path: str | Path,
    *,
    page_numbers: list[int] | None = None,
    table_settings: dict[str, object] | None = None,
) -> PDFTables:
    """Extract tables from a PDF using pdfplumber.

    Args:
        pdf_path: Path to the PDF file.
        page_numbers: 1-based page numbers to extract tables from. When None, all pages are scanned.
        table_settings: Settings passed directly to pdfplumber's extract_tables() method.

    Returns:
        PDFTables containing all extracted tables and a total count.

    Raises:
        PDFExtractionError: If the PDF cannot be opened or page numbers are invalid.
    """
    source_path = str(pdf_path)
    try:
        with pdfplumber.open(source_path) as pdf:
            total_pages = len(pdf.pages)

            if page_numbers is not None:
                _validate_page_numbers(page_numbers, total_pages, source_path)
                selected_indices = [page_number - 1 for page_number in page_numbers]
            else:
                selected_indices = list(range(total_pages))

            extracted_tables: list[PDFTable] = []
            for index in selected_indices:
                page = pdf.pages[index]
                if table_settings is not None:
                    raw_tables = page.extract_tables(table_settings)
                else:
                    raw_tables = page.extract_tables()

                for raw_table in raw_tables:
                    rows = tuple(tuple(cell for cell in row) for row in raw_table)
                    extracted_tables.append(PDFTable(page_number=index + 1, rows=rows))

            result = PDFTables(tables=tuple(extracted_tables), total_tables=len(extracted_tables))

            logger.info(
                f"Extracted tables from {pdf_path}: {result.total_tables} tables"
                f" from {len(selected_indices)} pages"
            )

            return result

    except PDFExtractionError:
        raise
    except Exception as error:
        raise PDFExtractionError(f"Failed to extract tables from PDF: {error}", source_path=source_path) from error


def _validate_page_numbers(page_numbers: list[int], total_pages: int, source_path: str) -> None:
    """Validate that all page numbers are within the valid 1-based range.

    Args:
        page_numbers: List of 1-based page numbers to validate.
        total_pages: Total number of pages in the PDF.
        source_path: Path to the PDF file (for error messages).

    Raises:
        PDFExtractionError: If any page number is out of range.
    """
    for page_number in page_numbers:
        if page_number < 1 or page_number > total_pages:
            raise PDFExtractionError(
                f"Page number {page_number} is out of range (1-{total_pages})",
                source_path=source_path,
            )
