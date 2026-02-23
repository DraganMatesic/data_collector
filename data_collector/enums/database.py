"""Database object type enums."""

from enum import IntEnum


class DbObjectType(IntEnum):
    """Supported routine object types for metadata mapping."""

    PROCEDURE = 1
    FUNCTION = 2
