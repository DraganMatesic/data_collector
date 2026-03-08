"""Web scraping engine -- convention-based lifecycle for data collection apps."""

from data_collector.scraping.async_scraper import AsyncScraper
from data_collector.scraping.base import (
    DEFAULT_CATEGORY_THRESHOLDS,
    BaseScraper,
    CategoryThreshold,
    update_app_status,
)
from data_collector.scraping.threaded import ThreadedScraper

__all__ = [
    "AsyncScraper",
    "BaseScraper",
    "CategoryThreshold",
    "DEFAULT_CATEGORY_THRESHOLDS",
    "ThreadedScraper",
    "update_app_status",
]
