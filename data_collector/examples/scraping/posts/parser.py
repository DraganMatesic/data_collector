"""Parser for jsonplaceholder.typicode.com API responses."""

from __future__ import annotations

from typing import Any

from data_collector.examples.scraping.posts.tables import ExamplePost


class Parser:
    """Parse JSON API responses into ExamplePost ORM objects."""

    def parse_post(self, data: dict[str, Any]) -> ExamplePost:
        """Convert a single post JSON object to an ORM record.

        Args:
            data: JSON dict with keys: id, userId, title, body.

        Returns:
            ExamplePost ORM object.
        """
        return ExamplePost(
            post_id=data["id"],
            user_id=data["userId"],
            title=str(data["title"]),
            body=str(data["body"]),
        )

    def parse_post_list(self, data: list[dict[str, Any]]) -> list[int]:
        """Extract post IDs from the /posts listing.

        Args:
            data: JSON list of post objects.

        Returns:
            List of post IDs.
        """
        return [int(post["id"]) for post in data]
