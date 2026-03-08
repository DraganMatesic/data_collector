"""Scraping-related enums for error categorization."""

from enum import StrEnum


class ErrorCategory(StrEnum):
    """Classifies scraper errors by operational severity.

    Used with CategoryThreshold in BaseScraper for per-category failure
    thresholds. Maps from RequestErrorType (HTTP-level) and explicit
    scraper-level classifications (database, I/O, captcha, parse).
    """

    DATABASE = "database"
    IO_WRITE = "io_write"
    CAPTCHA = "captcha"
    PROXY = "proxy"
    HTTP = "http"
    PARSE = "parse"
    UNKNOWN = "unknown"
