"""Shared ORM mixin models used by declarative table bases."""

from __future__ import annotations

from typing import Any


class BaseModel:
    """Provide readable `repr`/`str` for SQLAlchemy model instances."""

    def __repr__(self) -> str:
        cls = self.__class__.__name__
        table: Any = getattr(self, "__table__", None)
        if table is not None:
            attrs = ", ".join(
                f"{col.name}={getattr(self, col.name)!r}"
                for col in table.columns
            )
            return f"<{cls}({attrs})>"
        return f"<{cls}>"

    def __str__(self) -> str:
        return self.__repr__()
