"""
GitHub Public Repository REST API Client.
Read-only client supporting repository inspection, commits pagination,
commit detail & file diffs, pull requests, and rate-limit detection.
"""

import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
import httpx

logger = logging.getLogger(__name__)


def parse_github_url(url_or_slug: str) -> Tuple[str, str]:
    """
    Extract (owner, repo) from URLs or slugs.
    Examples:
      - 'https://github.com/apache/kafka' -> ('apache', 'kafka')
      - 'http://github.com/apache/kafka.git' -> ('apache', 'kafka')
      - 'github.com/apache/kafka' -> ('apache', 'kafka')
      - 'apache/kafka' -> ('apache', 'kafka')
    """
    cleaned = url_or_slug.strip().rstrip("/")
    if cleaned.endswith(".git"):
        cleaned = cleaned[:-4]

    # Handle full URLs (http/https)
    if cleaned.startswith("http://") or cleaned.startswith("https://"):
        parsed = urlparse(cleaned)
        netloc = (parsed.netloc or "").lower()
        if not (netloc == "github.com" or netloc.endswith(".github.com")):
            raise ValueError(f"URL is not a GitHub repository: '{url_or_slug}'")
        parts = [p for p in parsed.path.strip("/").split("/") if p]
        if len(parts) >= 2:
            return parts[0], parts[1]
        raise ValueError(f"Could not parse owner and repo from URL: '{url_or_slug}'")

    if cleaned.startswith("github.com/"):
        parts = cleaned.split("/")
        if len(parts) >= 3:
            return parts[1], parts[2]
        raise ValueError(f"Could not parse owner and repo from '{url_or_slug}'")

    parts = cleaned.split("/")
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[0], parts[1]

    raise ValueError(f"Invalid GitHub repository identifier or URL: '{url_or_slug}'")


class GitHubClient:
    def __init__(
        self,
        token: Optional[str] = None,
        base_url: str = "https://api.github.com",
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries

        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "TemporalProductKnowledge/1.0",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        self._headers = headers

    def _get_client(self) -> httpx.Client:
        return httpx.Client(headers=self._headers, timeout=self.timeout)

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{path}"
        with self._get_client() as client:
            retries = 0
            while True:
                response = client.request(method, url, params=params)
                if response.status_code in (403, 429):
                    # Check GitHub rate limit
                    remaining = response.headers.get("x-ratelimit-remaining")
                    reset_time = response.headers.get("x-ratelimit-reset")
                    if remaining == "0" and reset_time:
                        sleep_seconds = max(1, int(reset_time) - int(time.time()))
                        retries += 1
                        if retries > self.max_retries or sleep_seconds > 60:
                            raise RuntimeError(
                                f"GitHub API rate limit exceeded. Resets in {sleep_seconds} seconds. "
                                f"Provide a personal access token for higher limits."
                            )
                        logger.warning("GitHub rate limit hit. Waiting %d seconds...", sleep_seconds)
                        time.sleep(sleep_seconds)
                        continue

                response.raise_for_status()
                return response.json()

    def get_repo(self, owner: str, repo: str) -> Dict[str, Any]:
        """Fetch repository details."""
        return self._request("GET", f"/repos/{owner}/{repo}")

    def list_commits(
        self,
        owner: str,
        repo: str,
        sha: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        page: int = 1,
        per_page: int = 30,
    ) -> List[Dict[str, Any]]:
        """List repository commits with pagination."""
        params: Dict[str, Any] = {"page": page, "per_page": min(per_page, 100)}
        if sha:
            params["sha"] = sha
        if since:
            params["since"] = since
        if until:
            params["until"] = until
        return self._request("GET", f"/repos/{owner}/{repo}/commits", params=params)

    def get_commit(self, owner: str, repo: str, ref: str) -> Dict[str, Any]:
        """Fetch single commit details including changed files and diff stats."""
        return self._request("GET", f"/repos/{owner}/{repo}/commits/{ref}")

    def list_pull_requests(
        self,
        owner: str,
        repo: str,
        state: str = "all",
        page: int = 1,
        per_page: int = 30,
    ) -> List[Dict[str, Any]]:
        """List pull requests with pagination."""
        params = {"state": state, "page": page, "per_page": min(per_page, 100)}
        return self._request("GET", f"/repos/{owner}/{repo}/pulls", params=params)
