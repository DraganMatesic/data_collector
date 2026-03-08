"""Proxy management package for IP rotation and ban avoidance."""

from data_collector.proxy.blacklist import BlacklistChecker
from data_collector.proxy.judges import PROXY_JUDGES, extract_ip, verify_ip
from data_collector.proxy.models import Proxy, ProxyAcquisitionTimeout, ProxyData, extract_root_domain
from data_collector.proxy.provider import BrightDataProvider, ProxyProvider
from data_collector.proxy.proxy_manager import ProxyManager

__all__ = [
    "PROXY_JUDGES",
    "BlacklistChecker",
    "BrightDataProvider",
    "Proxy",
    "ProxyAcquisitionTimeout",
    "ProxyData",
    "ProxyManager",
    "ProxyProvider",
    "extract_ip",
    "extract_root_domain",
    "verify_ip",
]
