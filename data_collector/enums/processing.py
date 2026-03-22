"""Processing-related enums for PDF/OCR document classification."""

from enum import StrEnum


class PdfDataType(StrEnum):
    """Classification of a PDF document's extractable content type.

    Used by ``classify_pdf()`` to determine the processing path:

        TEXT  -- 90%+ pages have extractable text -> pdfplumber extraction.
        IMAGE -- Otherwise -> OCR engine extraction.

    """

    TEXT = "text"
    IMAGE = "image"


class DocumentType(StrEnum):
    """File format classification for ingested documents.

    Reserved for future use in pipeline routing and storage organization.
    """

    PDF = "pdf"
    XML = "xml"
    DOC = "doc"
    DOCX = "docx"
    XLS = "xls"
    XLSX = "xlsx"
    CSV = "csv"
    HTML = "html"
    TXT = "txt"
