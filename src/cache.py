"""
Simple disk-based JSON cache for HTTP responses.

Usage:
    cache = DiskCache("cache/http")
    data = cache.get("my_key")
    if data is None:
        data = fetch_something()
        cache.set("my_key", data)
"""

from __future__ import annotations
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class DiskCache:
    def __init__(self, cache_dir: str | Path, ttl_days: int = 30):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_seconds = ttl_days * 86_400

    def _path(self, key: str) -> Path:
        h = hashlib.sha256(key.encode()).hexdigest()
        return self.cache_dir / h[:2] / f"{h}.json"

    def get(self, key: str) -> Optional[Any]:
        import time
        p = self._path(key)
        if not p.exists():
            return None
        try:
            payload = json.loads(p.read_text("utf-8"))
            if time.time() - payload["ts"] > self.ttl_seconds:
                p.unlink(missing_ok=True)
                return None
            return payload["data"]
        except Exception as exc:
            logger.debug("Cache read error for %s: %s", key, exc)
            return None

    def set(self, key: str, data: Any) -> None:
        import time
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        try:
            p.write_text(json.dumps({"ts": time.time(), "data": data}), "utf-8")
        except Exception as exc:
            logger.debug("Cache write error for %s: %s", key, exc)

    def invalidate(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)
