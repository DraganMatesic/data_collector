"""Parser for books.toscrape.com catalogue pages."""

from __future__ import annotations

import re
from decimal import Decimal

from data_collector.examples.scraping.books.tables import ExampleBook

RATING_MAP = {
    "one": "1",
    "two": "2",
    "three": "3",
    "four": "4",
    "five": "5",
}


class Parser:
    """Parse book catalogue HTML into ExampleBook ORM objects."""

    def parse_catalogue(self, content: bytes) -> list[ExampleBook]:
        """Extract book records from a catalogue page.

        Uses regex parsing to avoid BeautifulSoup dependency in examples.

        Args:
            content: Raw HTML bytes from a catalogue page.

        Returns:
            List of ExampleBook ORM objects.
        """
        html = content.decode("utf-8", errors="replace")
        books: list[ExampleBook] = []

        for match in re.finditer(
            r'<article class="product_pod">.*?'
            r'class="star-rating (\w+)".*?'
            r'<h3><a[^>]+title="([^"]+)".*?'
            r'<p class="price_color">[^0-9]*([0-9.]+)',
            html,
            re.DOTALL,
        ):
            rating_word = match.group(1).lower()
            title = match.group(2)
            price_str = match.group(3)

            books.append(
                ExampleBook(
                    title=title,
                    price=Decimal(price_str),
                    rating=RATING_MAP.get(rating_word, rating_word),
                )
            )

        return books
