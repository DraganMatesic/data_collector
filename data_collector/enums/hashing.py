"""Hash-related enum values."""

from enum import StrEnum


class UnicodeForm(StrEnum):
    """Unicode normalization strategies."""

    NFC = "NFC"
    NFD = "NFD"
    NFKC = "NFKC"
    NFKD = "NFKD"
