"""Parser for quotes.toscrape.com pages and author detail pages."""

from __future__ import annotations

import re

from data_collector.examples.scraping.quotes_authors.tables import ExampleQuoteAuthor


class Parser:
    """Parse quote listings and author detail pages from quotes.toscrape.com."""

    def parse_quotes(self, content: bytes) -> list[ExampleQuoteAuthor]:
        """Extract quote records from a listing page.

        Args:
            content: Raw HTML bytes from a quotes page.

        Returns:
            List of ExampleQuoteAuthor ORM objects (without author details).
        """
        html = content.decode("utf-8", errors="replace")
        quotes: list[ExampleQuoteAuthor] = []

        for match in re.finditer(
            r'<div class="quote"[^>]*>.*?'
            r'<span class="text"[^>]*>\u201c([^\u201d]+)\u201d</span>.*?'
            r'<small class="author"[^>]*>([^<]+)</small>.*?'
            r'<div class="tags">(.*?)</div>',
            html,
            re.DOTALL,
        ):
            text = match.group(1).strip()
            author = match.group(2).strip()
            tags_html = match.group(3)
            tag_list = re.findall(r'class="tag">([^<]+)<', tags_html)
            tags = ", ".join(tag_list)

            quotes.append(
                ExampleQuoteAuthor(
                    text=text,
                    author=author,
                    tags=tags,
                )
            )

        return quotes

    def parse_author_urls(self, content: bytes, base_url: str) -> list[str]:
        """Extract unique author detail page URLs from a listing page.

        Args:
            content: Raw HTML bytes from a quotes page.
            base_url: Site root URL for building absolute URLs.

        Returns:
            List of unique absolute author page URLs.
        """
        html = content.decode("utf-8", errors="replace")
        urls: list[str] = []
        seen: set[str] = set()

        for match in re.finditer(r'<a href="(/author/[^"]+)"', html):
            path = match.group(1)
            if path not in seen:
                seen.add(path)
                urls.append(f"{base_url}{path}")

        return urls

    def parse_author_detail(self, content: bytes) -> dict[str, str]:
        """Extract author biography from an author detail page.

        Args:
            content: Raw HTML bytes from an author page.

        Returns:
            Dict with born_date, born_location, description keys.
        """
        html = content.decode("utf-8", errors="replace")
        result: dict[str, str] = {}

        born_date_match = re.search(
            r'<span class="author-born-date">([^<]+)</span>', html,
        )
        if born_date_match:
            result["born_date"] = born_date_match.group(1).strip()

        born_location_match = re.search(
            r'<span class="author-born-location">([^<]+)</span>', html,
        )
        if born_location_match:
            result["born_location"] = born_location_match.group(1).strip()

        description_match = re.search(
            r'<div class="author-description"[^>]*>(.*?)</div>', html, re.DOTALL,
        )
        if description_match:
            result["description"] = description_match.group(1).strip()

        return result

    def extract_author_name_from_url(self, url: str) -> str:
        """Extract author name from URL path.

        Args:
            url: Author detail page URL (e.g., https://quotes.toscrape.com/author/Albert-Einstein).

        Returns:
            Author name with hyphens replaced by spaces.
        """
        # /author/Albert-Einstein -> Albert Einstein
        path = url.rstrip("/").rsplit("/", maxsplit=1)[-1]
        return path.replace("-", " ")
