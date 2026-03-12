"""Captcha solving package with pluggable provider architecture."""

from data_collector.captcha.anti_captcha import AntiCaptchaProvider
from data_collector.captcha.metrics import CaptchaMetrics
from data_collector.captcha.models import CaptchaError, CaptchaResult, CaptchaTaskType, CaptchaTimeout
from data_collector.captcha.provider import BaseCaptchaProvider
from data_collector.enums.captcha import CaptchaErrorCategory, CaptchaSolveStatus

__all__ = [
    "AntiCaptchaProvider",
    "BaseCaptchaProvider",
    "CaptchaError",
    "CaptchaErrorCategory",
    "CaptchaMetrics",
    "CaptchaResult",
    "CaptchaSolveStatus",
    "CaptchaTaskType",
    "CaptchaTimeout",
]
