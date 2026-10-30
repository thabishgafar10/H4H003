"""
Jira Cloud REST API v3 Client.
Read-only client supporting issue search pagination, comments, changelogs, and rate-limit handling.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional
import httpx

from app.integrations.jira.auth import JiraAuthHandler

logger = logging.getLogger(__name__)


class JiraCloudClient:
    def __init__(
        self,
        base_url: str,
        email: Optional[str] = None,
        api_token: Optional[str] = None,
        oauth_token: Optional[str] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "TemporalProductKnowledge/1.0",
        }

        if oauth_token:
            headers.update(JiraAuthHandler.get_oauth_header(oauth_token))
        elif email and api_token:
            headers.update(JiraAuthHandler.get_basic_auth_header(email, api_token))

        self._headers = headers

    def _get_client(self) -> httpx.Client:
        return httpx.Client(headers=self._headers, timeout=self.timeout)

    def _request_with_retry(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        with self._get_client() as client:
            retries = 0
            while True:
                response = client.request(method, url, params=params)
                if response.status_code == 429:
                    retries += 1
                    if retries > self.max_retries:
                        response.raise_for_status()
                    retry_after = int(response.headers.get("Retry-After", 2 ** retries))
                    logger.warning("Jira rate limit hit (429). Retrying after %d seconds...", retry_after)
                    import time
                    time.sleep(min(retry_after, 30))
                    continue

                response.raise_for_status()
                return response.json()

    def test_connection(self) -> Dict[str, Any]:
        """Validate credentials and connectivity against /rest/api/3/myself."""
        return self._request_with_retry("GET", "/rest/api/3/myself")

    def get_project(self, project_key: str) -> Dict[str, Any]:
        """Fetch project metadata."""
        return self._request_with_retry("GET", f"/rest/api/3/project/{project_key}")

    def search_issues(
        self,
        jql: str,
        start_at: int = 0,
        max_results: int = 50,
        fields: Optional[List[str]] = None,
        expand: Optional[str] = "changelog",
    ) -> Dict[str, Any]:
        """
        Search issues using Jira Cloud v3 search API.
        Default expands changelog to capture transitions and historical field updates.
        """
        if fields is None:
            fields = [
                "summary",
                "description",
                "issuetype",
                "status",
                "priority",
                "resolution",
                "created",
                "updated",
                "resolutiondate",
                "creator",
                "reporter",
                "assignee",
                "components",
                "fixVersions",
                "issuelinks",
                "comment",
            ]

        params = {
            "jql": jql,
            "startAt": start_at,
            "maxResults": max_results,
            "fields": ",".join(fields),
        }
        if expand:
            params["expand"] = expand

        return self._request_with_retry("GET", "/rest/api/3/search", params=params)

    def get_issue_comments(self, issue_id_or_key: str, start_at: int = 0, max_results: int = 50) -> Dict[str, Any]:
        """Fetch paginated comments for an issue."""
        params = {"startAt": start_at, "maxResults": max_results}
        return self._request_with_retry("GET", f"/rest/api/3/issue/{issue_id_or_key}/comment", params=params)

    def get_issue_changelog(self, issue_id_or_key: str, start_at: int = 0, max_results: int = 50) -> Dict[str, Any]:
        """Fetch paginated changelog entries for an issue."""
        params = {"startAt": start_at, "maxResults": max_results}
        return self._request_with_retry("GET", f"/rest/api/3/issue/{issue_id_or_key}/changelog", params=params)
