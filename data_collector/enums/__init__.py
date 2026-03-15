"""Public enum exports used across the framework."""

from data_collector.enums.captcha import CaptchaErrorCategory, CaptchaSolveStatus
from data_collector.enums.commands import CmdFlag, CmdName
from data_collector.enums.database import DbObjectType
from data_collector.enums.hashing import UnicodeForm
from data_collector.enums.logging import LogLevel
from data_collector.enums.notifications import AlertSeverity
from data_collector.enums.pipeline import PipelineStage, PipelineStatus
from data_collector.enums.runtime import AppType, FatalFlag, RunStatus, RuntimeExitCode
from data_collector.enums.scraping import ErrorCategory

__all__ = [
    "AppType",
    "AlertSeverity",
    "CaptchaErrorCategory",
    "CaptchaSolveStatus",
    "CmdFlag",
    "CmdName",
    "DbObjectType",
    "ErrorCategory",
    "FatalFlag",
    "LogLevel",
    "PipelineStage",
    "PipelineStatus",
    "RunStatus",
    "RuntimeExitCode",
    "UnicodeForm",
]
