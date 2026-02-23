"""Logging-level enum bridge to stdlib logging constants."""

import logging
from enum import IntEnum


class LogLevel(IntEnum):
    """Framework log-level values."""

    NOTSET = logging.NOTSET
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
