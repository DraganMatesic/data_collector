"""Parser for quotes.toscrape.com pages."""

from __future__ import annotations

import re

from data_collector.examples.scraping.quotes.tables import ExampleQuote


class Parser:
    """Parse quote listings from quotes.toscrape.com HTML."""

    def parse_quotes(self, content: bytes) -> list[ExampleQuote]:
        """Extract quote records from a page.

        Uses regex parsing to avoid BeautifulSoup dependency in examples.

        Args:
            content: Raw HTML bytes from a quotes page.

        Returns:
            List of ExampleQuote ORM objects.
        """
        html = content.decode("utf-8", errors="replace")
        quotes: list[ExampleQuote] = []

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
                ExampleQuote(
                    text=text,
                    author=author,
                    tags=tags,
                )
            )

        return quotes
