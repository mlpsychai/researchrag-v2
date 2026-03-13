"""
Abstract base class for all academic API clients.
Provides shared HTTP session, rate limiting, and retry logic.
"""
import time
import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any

import requests

from models.paper import Paper

logger = logging.getLogger(__name__)


class RateLimitError(Exception):
    pass


class APIError(Exception):
    pass


class BaseAcademicClient(ABC):
    MIN_INTERVAL: float = 1.0
    SOURCE_NAME: str = "base"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self._last_call_time: float = 0.0
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "researchrag/0.1 (academic research tool)",
        })
        if api_key:
            self._configure_auth(api_key)

    def _configure_auth(self, api_key: str) -> None:
        pass

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_call_time
        wait = self.MIN_INTERVAL - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_call_time = time.monotonic()

    def _get(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        self._throttle()
        response = self.session.get(url, params=params, timeout=15)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "10"))
            logger.warning(f"{self.SOURCE_NAME}: rate limited, waiting {retry_after}s")
            time.sleep(retry_after)
            raise RateLimitError(f"Rate limited by {self.SOURCE_NAME}")

        if response.status_code >= 500:
            raise requests.exceptions.ConnectionError(
                f"{self.SOURCE_NAME}: server error {response.status_code}"
            )

        if response.status_code == 404:
            return {}

        response.raise_for_status()
        return response.json()

    def _get_with_retry(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        for attempt in range(3):
            try:
                return self._get(url, params)
            except (RateLimitError, requests.exceptions.ConnectionError) as e:
                if attempt == 2:
                    raise APIError(f"{self.SOURCE_NAME} failed after 3 attempts: {e}")
                wait = 5 * (attempt + 1)  # 5s, 10s
                logger.warning(f"{self.SOURCE_NAME}: retry {attempt+1}/3 after {wait}s - {e}")
                time.sleep(wait)
        return {}

    @abstractmethod
    def search(self, query: str, max_results: int = 10) -> List[Paper]:
        ...

    @abstractmethod
    def get_paper(self, paper_id: str) -> Optional[Paper]:
        ...

    @abstractmethod
    def _normalize(self, raw: Dict[str, Any]) -> Paper:
        ...
