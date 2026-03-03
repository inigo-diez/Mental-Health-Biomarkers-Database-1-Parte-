"""
HTTP client with:
  - exponential back-off retries
  - global rate limiter (token bucket)
  - disk cache (via DiskCache)
"""

from __future__ import annotations
import logging
import threading
import time
from typing import Any, Optional

import requests

from src.cache import DiskCache

logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe token-bucket rate limiter."""

    def __init__(self, rate: float):
        self.rate = rate          # tokens per second
        self._tokens = rate
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self.rate, self._tokens + elapsed * self.rate)
            self._last = now
            if self._tokens < 1:
                sleep_for = (1 - self._tokens) / self.rate
                time.sleep(sleep_for)
                self._tokens = 0
            else:
                self._tokens -= 1


class HTTPClient:
    """Cached, rate-limited HTTP client."""

    def __init__(
        self,
        rate: float = 3.0,
        retry_max: int = 5,
        retry_backoff: float = 2.0,
        timeout: int = 30,
        cache_dir: str = "cache/http",
        cache_ttl_days: int = 7,
    ):
        self.limiter = RateLimiter(rate)
        self.retry_max = retry_max
        self.retry_backoff = retry_backoff
        self.timeout = timeout
        self.cache = DiskCache(cache_dir, ttl_days=cache_ttl_days)
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "MetaboliteCollector/1.0 (TFG research; contact: student)"}
        )

    def get(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        use_cache: bool = True,
        stream: bool = False,
    ) -> requests.Response:
        cache_key = url + str(sorted((params or {}).items()))

        if use_cache and not stream:
            cached = self.cache.get(cache_key)
            if cached is not None:
                logger.debug("Cache HIT: %s", url)
                # Return a mock-like object with .json() and .text
                return _CachedResponse(cached)

        attempt = 0
        while True:
            try:
                self.limiter.acquire()
                resp = self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                    stream=stream,
                )
                resp.raise_for_status()

                if use_cache and not stream:
                    try:
                        self.cache.set(cache_key, {"json": resp.json(), "text": resp.text})
                    except Exception:
                        self.cache.set(cache_key, {"text": resp.text, "json": None})

                return resp

            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if status in (429, 503) or attempt < self.retry_max:
                    wait = self.retry_backoff ** attempt
                    logger.warning("HTTP %s for %s — retry %d in %.1fs", status, url, attempt + 1, wait)
                    time.sleep(wait)
                    attempt += 1
                else:
                    raise

            except requests.RequestException as exc:
                if attempt < self.retry_max:
                    wait = self.retry_backoff ** attempt
                    logger.warning("Request error %s — retry %d in %.1fs", exc, attempt + 1, wait)
                    time.sleep(wait)
                    attempt += 1
                else:
                    raise

    def get_json(self, url: str, params: Optional[dict] = None, **kwargs) -> Any:
        resp = self.get(url, params=params, **kwargs)
        if isinstance(resp, _CachedResponse):
            return resp.json()
        return resp.json()

    def get_text(self, url: str, params: Optional[dict] = None, **kwargs) -> str:
        resp = self.get(url, params=params, **kwargs)
        if isinstance(resp, _CachedResponse):
            return resp.text
        return resp.text


class _CachedResponse:
    """Minimal duck-typed response wrapping cached data."""

    def __init__(self, payload: dict):
        self._payload = payload
        self.text = payload.get("text", "")
        self.status_code = 200

    def json(self) -> Any:
        j = self._payload.get("json")
        if j is not None:
            return j
        import json
        return json.loads(self.text)

    def raise_for_status(self) -> None:
        pass
