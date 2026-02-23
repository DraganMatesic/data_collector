"""Centralized HTTP client with retry logic, error tracking, and metrics.

Classes:
    ExceptionDescriptor — timestamped error tracking for time-based analysis.
    RequestMetrics — thread-safe shared metrics collector with circuit breaker.
    Request — httpx-based sync + async HTTP client.
"""


from __future__ import annotations

import asyncio
import logging
import random
import threading
import time
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, TypedDict, cast
from urllib.parse import urlparse

import httpx
import zeep
from zeep.exceptions import Fault, TransportError
from zeep.transports import Transport

# ---------------------------------------------------------------------------
# RequestErrorType
# ---------------------------------------------------------------------------

class RequestErrorType(StrEnum):
    """Error type classification for HTTP request failures."""

    TIMEOUT = "timeout"
    PROXY = "proxy"
    BAD_STATUS = "bad_status_code"
    REDIRECT = "redirect"
    REQUEST = "request"
    OTHER = "other"


# ---------------------------------------------------------------------------
# Internal TypedDicts for structured metrics data
# ---------------------------------------------------------------------------

class _ProxyStats(TypedDict):
    count: int
    success: int
    timings: list[float]


class _TargetFailure(TypedDict):
    failures: int
    proxies: set[str]


# ---------------------------------------------------------------------------
# ExceptionDescriptor
# ---------------------------------------------------------------------------

class ExceptionDescriptor:
    """Tracks errors with timestamps for time-based analysis."""

    def __init__(self) -> None:
        self.errors: dict[datetime, dict[str, str]] = {}

    def add_error(self, error_type: str, message: str, url: str | None = None) -> None:
        """Record an error with the current timestamp."""
        self.errors[datetime.now()] = {
            "type": error_type,
            "message": message,
            "url": url or "",
        }

    def get_last_error(self) -> dict[str, str] | None:
        """Return the most recent error dict, or None."""
        return next(reversed(self.errors.values()), None)

    def get_errors_by_type(self, error_type: str) -> list[dict[str, str]]:
        """Return all errors matching the given type string."""
        return [v for v in self.errors.values() if v["type"] == error_type]

    def has_errors_after(self, timestamp: datetime) -> bool:
        """Return True if any error was recorded after the given timestamp."""
        return any(err_time > timestamp for err_time in self.errors)

    def clear(self) -> None:
        """Remove all recorded errors."""
        self.errors.clear()


# ---------------------------------------------------------------------------
# RequestMetrics
# ---------------------------------------------------------------------------

class RequestMetrics:
    """Thread-safe shared metrics collector for multi-threaded HTTP operations.

    Created once per runtime and passed to every Request instance. Aggregates
    error counters, timing data, status codes, per-proxy stats, and circuit
    breaker state across all threads.
    """

    RESERVOIR_SIZE: int = 1000

    def __init__(
        self,
        max_target_failures: int = 3,
        min_distinct_proxies: int = 2,
    ) -> None:
        self._lock = threading.Lock()

        # Aggregated error counters
        self.request_count: int = 0
        self.timeout_err: int = 0
        self.proxy_err: int = 0
        self.bad_status_code_err: int = 0
        self.redirect_err: int = 0
        self.request_err: int = 0
        self.other_err: int = 0

        # Per-domain timing reservoir: {domain: [response_time_ms, ...]}
        self._domain_timings: dict[str, list[float]] = {}
        # Total requests per domain (needed for reservoir Algorithm R)
        self._domain_request_counts: dict[str, int] = {}

        # Per-domain status codes: {domain: {status_code: count}}
        self._domain_status_codes: dict[str, dict[int, int]] = {}

        # Per-proxy stats: {proxy_key: {"count": int, "success": int, "timings": list}}
        self._proxy_stats: dict[str, _ProxyStats] = {}

        # Circuit breaker: {domain: {"failures": int, "proxies": set}}
        self._target_failures: dict[str, _TargetFailure] = {}
        self._max_target_failures = max_target_failures
        self._min_distinct_proxies = min_distinct_proxies

    def record_request(
        self, domain: str, proxy: str | None, status_code: int, response_time_ms: float
    ) -> None:
        """Record a completed HTTP request. Called by Request._make_request()."""
        with self._lock:
            self.request_count += 1

            # Domain timing — reservoir sampling (Algorithm R)
            if domain not in self._domain_timings:
                self._domain_timings[domain] = []
                self._domain_request_counts[domain] = 0
            self._domain_request_counts[domain] += 1
            n = self._domain_request_counts[domain]
            reservoir = self._domain_timings[domain]
            if len(reservoir) < self.RESERVOIR_SIZE:
                reservoir.append(response_time_ms)
            else:
                j = random.randint(0, n - 1)
                if j < self.RESERVOIR_SIZE:
                    reservoir[j] = response_time_ms

            # Domain status codes
            if domain not in self._domain_status_codes:
                self._domain_status_codes[domain] = {}
            codes = self._domain_status_codes[domain]
            codes[status_code] = codes.get(status_code, 0) + 1

            # Per-proxy stats
            proxy_key = proxy or "direct"
            if proxy_key not in self._proxy_stats:
                self._proxy_stats[proxy_key] = {"count": 0, "success": 0, "timings": []}
            pstat = self._proxy_stats[proxy_key]
            pstat["count"] += 1
            if 200 <= status_code < 300:
                pstat["success"] += 1
            ptimings = pstat["timings"]
            if len(ptimings) < self.RESERVOIR_SIZE:
                ptimings.append(response_time_ms)
            else:
                j = random.randint(0, pstat["count"] - 1)
                if j < self.RESERVOIR_SIZE:
                    ptimings[j] = response_time_ms

            # Circuit breaker — reset on success (2xx)
            if 200 <= status_code < 300:
                self._target_failures.pop(domain, None)
            else:
                self._record_target_failure(domain, proxy_key)

            # Increment bad_status_code_err for non-2xx
            if not (200 <= status_code < 300):
                self.bad_status_code_err += 1

    def record_error(self, domain: str, proxy: str | None, error_type: str) -> None:
        """Record an HTTP error. Called by Request._make_request() on exception."""
        proxy_key = proxy or "direct"
        with self._lock:
            counter_name = f"{error_type}_err"
            if hasattr(self, counter_name):
                setattr(self, counter_name, getattr(self, counter_name) + 1)
            self._record_target_failure(domain, proxy_key)

    def _record_target_failure(self, domain: str, proxy_key: str) -> None:
        """Update circuit breaker state. Must be called with lock held."""
        if domain not in self._target_failures:
            self._target_failures[domain] = {"failures": 0, "proxies": set()}
        entry = self._target_failures[domain]
        entry["failures"] += 1
        entry["proxies"].add(proxy_key)

    def is_target_unhealthy(self, url: str) -> bool:
        """Circuit breaker check.

        Returns True if a target has failed >= max_target_failures times across
        >= min_distinct_proxies different proxies.
        """
        domain = urlparse(url).netloc
        with self._lock:
            entry = self._target_failures.get(domain)
            if entry is None:
                return False
            return (
                entry["failures"] >= self._max_target_failures
                and len(entry["proxies"]) >= self._min_distinct_proxies
            )

    def log_stats(self, logger: logging.Logger) -> dict[str, Any]:
        """Log and return aggregated statistics dictionary."""
        with self._lock:
            total_errors = (
                self.timeout_err + self.proxy_err + self.bad_status_code_err
                + self.redirect_err + self.request_err + self.other_err
            )
            error_rate = (total_errors / self.request_count * 100) if self.request_count > 0 else 0.0

            # Build error breakdown dynamically
            error_breakdown: dict[str, int] = {}
            for attr in ("timeout", "proxy", "bad_status_code", "redirect", "request", "other"):
                val: int = getattr(self, f"{attr}_err")
                if val > 0:
                    error_breakdown[attr] = val

            # Aggregate timing from all domains
            all_timings: list[float] = []
            for timings in self._domain_timings.values():
                all_timings.extend(timings)
            timing = self._compute_timing(all_timings)

            # By domain
            by_domain: dict[str, dict[str, Any]] = {}
            for domain, timings in self._domain_timings.items():
                domain_count = self._domain_request_counts.get(domain, 0)
                codes = self._domain_status_codes.get(domain, {})
                success = sum(v for k, v in codes.items() if 200 <= k < 300)
                by_domain[domain] = {
                    "count": domain_count,
                    "success": success,
                    "p95_ms": self._percentile(timings, 0.95),
                    "status_codes": {str(k): v for k, v in sorted(codes.items())},
                }

            # By proxy
            by_proxy: dict[str, dict[str, Any]] = {}
            for proxy_key, pstat in self._proxy_stats.items():
                by_proxy[proxy_key] = {
                    "count": pstat["count"],
                    "success": pstat["success"],
                    "p95_ms": self._percentile(pstat["timings"], 0.95),
                }

            stats: dict[str, Any] = {
                "total_requests": self.request_count,
                "total_errors": total_errors,
                "error_rate_percent": round(error_rate, 2),
                "error_breakdown": error_breakdown,
                "timing": timing,
                "by_domain": by_domain,
                "by_proxy": by_proxy,
            }

        logger.info("Request statistics: %s", stats)
        return stats

    @staticmethod
    def _compute_timing(timings: list[float]) -> dict[str, float | int]:
        """Compute avg/p50/p95/p99 from a timing list."""
        if not timings:
            return {"avg_ms": 0, "p50_ms": 0, "p95_ms": 0, "p99_ms": 0}
        return {
            "avg_ms": round(sum(timings) / len(timings)),
            "p50_ms": RequestMetrics._percentile(timings, 0.50),
            "p95_ms": RequestMetrics._percentile(timings, 0.95),
            "p99_ms": RequestMetrics._percentile(timings, 0.99),
        }

    @staticmethod
    def _percentile(data: list[float], pct: float) -> int:
        """Compute a percentile value from a list of numbers."""
        if not data:
            return 0
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * pct)
        idx = min(idx, len(sorted_data) - 1)
        return round(sorted_data[idx])


# ---------------------------------------------------------------------------
# Request
# ---------------------------------------------------------------------------

# Status codes that should not be retried
_NO_RETRY_STATUSES = {401, 403, 404}


class Request:
    """httpx-based sync + async HTTP client with retry logic and error tracking.

    Args:
        timeout: Request timeout in seconds.
        retries: Maximum retry attempts.
        backoff_factor: Exponential backoff multiplier.
        retry_on_status: HTTP status codes that trigger retry.
        save_responses: Save raw responses to disk.
        save_dir: Directory for saved responses.
        metrics: Shared RequestMetrics collector for multi-threaded aggregation.
    """

    def __init__(
        self,
        timeout: int = 30,
        retries: int = 3,
        backoff_factor: int = 2,
        retry_on_status: list[int] | None = None,
        save_responses: bool = False,
        save_dir: str | None = None,
        metrics: RequestMetrics | None = None,
    ) -> None:
        # Transport config
        self._timeout = timeout
        self._retries = retries
        self._backoff_factor = backoff_factor
        self._retry_on_status = retry_on_status or [429, 500, 502, 503, 504]
        self._save_responses = save_responses
        self._save_dir = save_dir
        self._metrics = metrics

        # Session state (mutable via setters)
        self._headers: dict[str, str] = {}
        self._cookies: dict[str, str] = {}
        self._auth: tuple[str, str] | None = None
        self._proxy: str | None = None

        # Per-instance error tracking
        self.exception_descriptor = ExceptionDescriptor()

        # Error counters (used when no metrics provided)
        self.request_count: int = 0
        self.timeout_err: int = 0
        self.proxy_err: int = 0
        self.bad_status_code_err: int = 0
        self.redirect_err: int = 0
        self.request_err: int = 0
        self.other_err: int = 0

        # Response state
        self.response: httpx.Response | None = None
        self.last_request_url: str | None = None
        self._last_request_time: datetime | None = None

        # SOAP client reference
        self._soap_client: Any = None

    # --- Setter methods ---

    def set_headers(self, headers: dict[str, str]) -> None:
        """Set default headers for all requests."""
        self._headers = dict(headers)

    def reset_headers(self) -> None:
        """Clear all default headers."""
        self._headers = {}

    def set_cookies(self, cookies: dict[str, str]) -> None:
        """Set cookies for session persistence."""
        self._cookies = dict(cookies)

    def reset_cookies(self) -> None:
        """Clear all cookies."""
        self._cookies = {}

    def set_auth(self, username: str, password: str) -> None:
        """Set HTTP basic authentication."""
        self._auth = (username, password)

    def set_proxy(self, proxy_config: str | None) -> None:
        """Set proxy configuration.

        Args:
            proxy_config: Proxy URL string (e.g. "http://user:pass@host:port")
                or None to clear.
        """
        self._proxy = proxy_config

    # --- HTTP methods ---

    def get(self, url: str, **kwargs: Any) -> httpx.Response | None:
        """Synchronous GET request."""
        return self._make_request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response | None:
        """Synchronous POST request."""
        return self._make_request("POST", url, **kwargs)

    async def async_get(self, url: str, **kwargs: Any) -> httpx.Response | None:
        """Asynchronous GET request."""
        return await self._async_make_request("GET", url, **kwargs)

    async def async_post(self, url: str, **kwargs: Any) -> httpx.Response | None:
        """Asynchronous POST request."""
        return await self._async_make_request("POST", url, **kwargs)

    # --- Internal request engine ---

    def _build_client_kwargs(self) -> dict[str, Any]:
        """Build kwargs for httpx.Client / httpx.AsyncClient."""
        kwargs: dict[str, Any] = {
            "timeout": self._timeout,
            "follow_redirects": True,
        }
        if self._headers:
            kwargs["headers"] = self._headers
        if self._cookies:
            kwargs["cookies"] = self._cookies
        if self._auth:
            kwargs["auth"] = self._auth
        if self._proxy:
            kwargs["proxy"] = self._proxy
        return kwargs

    def _make_request(self, method: str, url: str, **kwargs: Any) -> httpx.Response | None:
        """Core sync request logic with retry and exponential backoff."""
        self._last_request_time = datetime.now()
        self.last_request_url = url
        self.response = None
        domain = self._extract_domain(url)
        proxy_key = self._get_proxy_key()

        client_kwargs = self._build_client_kwargs()

        for attempt in range(self._retries + 1):
            try:
                start = time.monotonic()
                with httpx.Client(**client_kwargs) as client:
                    self.response = client.request(method, url, **kwargs)
                elapsed_ms = (time.monotonic() - start) * 1000

                self.request_count += 1
                if self._metrics:
                    self._metrics.record_request(domain, proxy_key, self.response.status_code, elapsed_ms)

                status = self.response.status_code

                # Success
                if 200 <= status < 300:
                    if self._save_responses:
                        self._auto_save_response(url)
                    return self.response

                # No retry for certain status codes
                if status in _NO_RETRY_STATUSES:
                    self._record_error(RequestErrorType.BAD_STATUS, f"HTTP {status}", url)
                    return self.response

                # Retryable status
                if status in self._retry_on_status and attempt < self._retries:
                    time.sleep(self._backoff_factor ** attempt)
                    continue

                # Non-retryable non-2xx
                self._record_error(RequestErrorType.BAD_STATUS, f"HTTP {status}", url)
                return self.response

            except Exception as exc:
                error_type, retryable = self._classify_exception(exc)
                self._record_error(error_type, str(exc), url)
                if retryable and attempt < self._retries:
                    time.sleep(self._backoff_factor ** attempt)
                    continue
                if self._metrics:
                    self._metrics.record_error(domain, proxy_key, error_type)
                return None

        return self.response

    async def _async_make_request(self, method: str, url: str, **kwargs: Any) -> httpx.Response | None:
        """Core async request logic with retry and exponential backoff."""
        self._last_request_time = datetime.now()
        self.last_request_url = url
        self.response = None
        domain = self._extract_domain(url)
        proxy_key = self._get_proxy_key()

        client_kwargs = self._build_client_kwargs()

        for attempt in range(self._retries + 1):
            try:
                start = time.monotonic()
                async with httpx.AsyncClient(**client_kwargs) as client:
                    self.response = await client.request(method, url, **kwargs)
                elapsed_ms = (time.monotonic() - start) * 1000

                self.request_count += 1
                if self._metrics:
                    self._metrics.record_request(domain, proxy_key, self.response.status_code, elapsed_ms)

                status = self.response.status_code

                if 200 <= status < 300:
                    if self._save_responses:
                        self._auto_save_response(url)
                    return self.response

                if status in _NO_RETRY_STATUSES:
                    self._record_error(RequestErrorType.BAD_STATUS, f"HTTP {status}", url)
                    return self.response

                if status in self._retry_on_status and attempt < self._retries:
                    await asyncio.sleep(self._backoff_factor ** attempt)
                    continue

                self._record_error(RequestErrorType.BAD_STATUS, f"HTTP {status}", url)
                return self.response

            except Exception as exc:
                error_type, retryable = self._classify_exception(exc)
                self._record_error(error_type, str(exc), url)
                if retryable and attempt < self._retries:
                    await asyncio.sleep(self._backoff_factor ** attempt)
                    continue
                if self._metrics:
                    self._metrics.record_error(domain, proxy_key, error_type)
                return None

        return self.response

    def _record_error(self, error_type: str, message: str, url: str) -> None:
        """Record error in ExceptionDescriptor and increment local counter."""
        self.exception_descriptor.add_error(error_type, message, url)
        counter_name = f"{error_type}_err"
        if hasattr(self, counter_name):
            setattr(self, counter_name, getattr(self, counter_name) + 1)

    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract domain (netloc) from URL for metrics keying."""
        return urlparse(url).netloc

    def _get_proxy_key(self) -> str | None:
        """Return sanitized proxy identifier for metrics.

        Strips credentials, returns host:port only.
        """
        if not self._proxy:
            return None
        try:
            parsed = urlparse(self._proxy)
            host = parsed.hostname or ""
            port = parsed.port
            return f"{host}:{port}" if port else host
        except Exception:
            return "unknown"

    def _classify_exception(self, exc: Exception) -> tuple[RequestErrorType, bool]:
        """Classify an exception into (error_type, is_retryable).

        Returns:
            Tuple of (error_type constant, whether the error is retryable).
        """
        if isinstance(exc, httpx.TimeoutException):
            return RequestErrorType.TIMEOUT, True
        if isinstance(exc, (httpx.ConnectError, httpx.ProxyError)):
            return RequestErrorType.PROXY, True
        if isinstance(exc, httpx.TooManyRedirects):
            return RequestErrorType.REDIRECT, False
        if isinstance(exc, httpx.HTTPError):
            return RequestErrorType.REQUEST, False
        return RequestErrorType.OTHER, False

    # --- Error introspection ---

    def has_errors(self) -> bool:
        """True if any error occurred after the last request timestamp."""
        if self._last_request_time is None:
            return False
        return self.exception_descriptor.has_errors_after(self._last_request_time)

    def is_blocked(self) -> bool:
        """True if the last error indicates IP block (401/403/429 or forcibly closed)."""
        last = self.exception_descriptor.get_last_error()
        if last is None:
            return False
        msg = last.get("message", "").lower()
        return (
            "401" in msg or "403" in msg or "429" in msg
            or "forcibly closed" in msg
        )

    def is_proxy_error(self) -> bool:
        """True if the last error is a proxy/connection/SSL error."""
        last = self.exception_descriptor.get_last_error()
        if last is None:
            return False
        return last.get("type") == RequestErrorType.PROXY

    def is_timeout(self) -> bool:
        """True if the last error is a timeout."""
        last = self.exception_descriptor.get_last_error()
        if last is None:
            return False
        return last.get("type") == RequestErrorType.TIMEOUT

    def is_server_down(self) -> bool:
        """True if the last error indicates a 5xx server error."""
        last = self.exception_descriptor.get_last_error()
        if last is None:
            return False
        msg = last.get("message", "")
        return any(str(code) in msg for code in range(500, 600))

    # --- Abort decision ---

    def should_abort(self, logger: logging.Logger, proxy_on: bool = False) -> bool:
        """Returns True if a critical error occurred and the caller should stop.

        Args:
            logger: Logger instance for recording abort reason.
            proxy_on: Whether proxy is active (enables proxy-specific checks).
        """
        if not self.has_errors():
            return False

        if proxy_on and (self.is_blocked() or self.is_proxy_error()):
            logger.debug("Aborting: proxy is blocked or not working")
            return True

        if self.is_timeout():
            logger.debug("Aborting: page or proxy timeout")
            return True

        if self.is_server_down():
            logger.debug("Aborting: server page is down")
            return True

        last = self.exception_descriptor.get_last_error()
        logger.error(str(last))
        return True

    # --- Circuit breaker delegation ---

    def is_target_unhealthy(self, url: str) -> bool:
        """Delegates to metrics.is_target_unhealthy() or returns False."""
        if self._metrics is None:
            return False
        return self._metrics.is_target_unhealthy(url)

    # --- Response helpers ---

    def save_html(self, save_path: str) -> None:
        """Save response content to file (binary write)."""
        if self.response is None:
            return
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        Path(save_path).write_bytes(self.response.content)

    def get_content(self) -> bytes | None:
        """Return raw response content."""
        if self.response is None:
            return None
        return self.response.content

    def get_content_length(self) -> int | None:
        """Return Content-Length header as int."""
        if self.response is None:
            return None
        length = self.response.headers.get("content-length")
        if length is not None:
            try:
                return int(length)
            except ValueError:
                return None
        return None

    def get_json(self) -> dict[str, Any] | str | None:
        """Parse response as JSON, return error string on failure."""
        if self.response is None:
            return None
        try:
            return cast(dict[str, Any] | str | None, self.response.json())
        except Exception as exc:
            return str(exc)

    # --- Statistics ---

    def log_stats(self, logger: logging.Logger) -> dict[str, Any]:
        """Log and return statistics.

        When RequestMetrics is provided, delegates to metrics.log_stats().
        Otherwise returns basic error counters from this instance.
        """
        if self._metrics:
            return self._metrics.log_stats(logger)

        total_errors = (
            self.timeout_err + self.proxy_err + self.bad_status_code_err
            + self.redirect_err + self.request_err + self.other_err
        )
        error_rate = (total_errors / self.request_count * 100) if self.request_count > 0 else 0.0

        error_breakdown: dict[str, int] = {}
        for attr in ("timeout", "proxy", "bad_status_code", "redirect", "request", "other"):
            val: int = getattr(self, f"{attr}_err")
            if val > 0:
                error_breakdown[attr] = val

        stats: dict[str, Any] = {
            "total_requests": self.request_count,
            "total_errors": total_errors,
            "error_rate_percent": round(error_rate, 2),
            "error_breakdown": error_breakdown,
        }
        logger.info("Request statistics: %s", stats)
        return stats

    # --- Response saving ---

    def _auto_save_response(self, url: str) -> None:
        """Auto-save response when save_responses is enabled."""
        if not self._save_dir or self.response is None:
            return

        content_type = self.response.headers.get("content-type", "")
        ext = ".json" if "json" in content_type else ".html"

        parsed = urlparse(url)
        domain = parsed.netloc.replace(":", "_")
        path_part = parsed.path.strip("/").replace("/", "_") or "index"
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        filename = f"{timestamp}_{domain}_{path_part}{ext}"

        save_dir = Path(self._save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)
        (save_dir / filename).write_bytes(self.response.content)

    # --- SOAP ---

    def create_soap_client(self, wsdl_url: str, **kwargs: Any) -> Any:
        """Create a Zeep SOAP client wired through this Request's session.

        Args:
            wsdl_url: URL of the WSDL document.
            **kwargs: Additional kwargs passed to zeep.Client (e.g. wsse, cache).

        Returns:
            zeep.Client instance.
        """
        session = httpx.Client(**self._build_client_kwargs())
        # Zeep requires a requests-compatible session; use zeep's own transport
        # with timeout from our config
        transport = Transport(timeout=self._timeout, operation_timeout=self._timeout)  # type: ignore[no-untyped-call]
        cache = kwargs.pop("cache", False)
        if cache:
            transport.cache = zeep.cache.InMemoryCache()  # type: ignore[no-untyped-call]

        self._soap_client = zeep.Client(wsdl_url, transport=transport, **kwargs)  # type: ignore[no-untyped-call]
        session.close()
        return self._soap_client

    def soap_call(self, service_method: Any, raise_faults: bool = False, **params: Any) -> Any:
        """Call a SOAP service method with error handling.

        Args:
            service_method: Zeep service method to call (e.g. client.service.GetData).
            raise_faults: If True, re-raise SOAP Fault exceptions.
            **params: Parameters to pass to the service method.

        Returns:
            Result from the SOAP call, or None on error.
        """
        self._last_request_time = datetime.now()
        try:
            start = time.monotonic()
            result = service_method(**params)
            elapsed_ms = (time.monotonic() - start) * 1000

            self.request_count += 1
            if self._metrics:
                self._metrics.record_request("soap", self._get_proxy_key(), 200, elapsed_ms)
            return result

        except Fault as exc:
            self._record_error(RequestErrorType.REQUEST, f"SOAP Fault: {exc.message}", "soap")
            if self._metrics:
                self._metrics.record_error("soap", self._get_proxy_key(), RequestErrorType.REQUEST)
            if raise_faults:
                raise
            return None

        except TransportError as exc:
            self._record_error(RequestErrorType.PROXY, f"SOAP TransportError: {exc}", "soap")
            if self._metrics:
                self._metrics.record_error("soap", self._get_proxy_key(), RequestErrorType.PROXY)
            return None

        except Exception as exc:
            error_type = RequestErrorType.TIMEOUT if "timeout" in str(exc).lower() else RequestErrorType.OTHER
            self._record_error(error_type, str(exc), "soap")
            if self._metrics:
                self._metrics.record_error("soap", self._get_proxy_key(), error_type)
            return None
