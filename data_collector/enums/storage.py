"""Storage-related enums for file retention classification."""

from enum import IntEnum


class FileRetention(IntEnum):
    """Default retention categories for stored files.

    Each member maps to a ``CodebookFileRetention`` row whose
    ``retention_days`` value determines the actual retention period.
    The database is the source of truth -- these enum values are
    default seed IDs.  Companies can add custom categories by
    inserting additional rows into the codebook table.

    ``PERMANENT`` files have ``retention_days=NULL`` and are never
    deleted by retention enforcement.
    """

    TRANSIENT = 1
    SHORT_TERM = 2
    STANDARD = 3
    REGULATORY_3Y = 4
    REGULATORY_5Y = 5
    REGULATORY_7Y = 6
    REGULATORY_10Y = 7
    EXTENDED = 8
    PERMANENT = 9
