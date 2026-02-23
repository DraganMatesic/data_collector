"""Command-related enums for app control flow."""

from enum import IntEnum


class CmdFlag(IntEnum):
    """Execution state of a queued command."""

    PENDING = 0
    EXECUTED = 1
    NOT_EXECUTED = 2


class CmdName(IntEnum):
    """Supported command identifiers."""

    START = 1
    STOP = 2
    RESTART = 3
    ENABLE = 4
    DISABLE = 5
