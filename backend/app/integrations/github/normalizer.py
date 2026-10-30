"""
GitHub Data Normalizer.
Converts raw GitHub REST API commit, file diff, and pull request payloads
into normalized domain entities and SourceRecord provenance entries:
- Commit
- CodeChange
- CommitIssueLink
- SourceRecord (commits and PRs)
"""

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.models.code_change import CodeChange
from app.models.commit import Commit
from app.models.issue_relationships import CommitIssueLink
from app.models.source_record import SourceRecord


def parse_github_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        return datetime.fromisoformat(dt_str)
    except Exception:
        try:
            return datetime.strptime(dt_str.split(".")[0], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None


def calculate_sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class GitHubNormalizer:
    # Pattern to match issue keys like KAFKA-1234 or PROJ-99
    ISSUE_PATTERN = re.compile(r"\b([A-Z][A-Z0-9_]{1,15}-\d+)\b")

    @classmethod
    def normalize_commit(
        cls,
        commit_data: Dict[str, Any],
        project_id: int,
        dataset_name: str,
    ) -> Tuple[Commit, SourceRecord, List[CodeChange], List[CommitIssueLink]]:
        sha = commit_data.get("sha", "")
        commit_info = commit_data.get("commit", {})
        author_info = commit_info.get("author") or {}
        committer_info = commit_info.get("committer") or {}

        message = commit_info.get("message") or ""
        author = author_info.get("name") or "Unknown"
        author_email = author_info.get("email")
        author_date = parse_github_datetime(author_info.get("date"))
        commit_date = parse_github_datetime(committer_info.get("date")) or author_date

        commit = Commit(
            commit_hash=sha,
            message=message,
            author=author,
            author_email=author_email,
            committed_at=commit_date,
            is_merge=len(commit_data.get("parents", [])) > 1,
        )

        raw_payload = json.dumps(commit_data, sort_keys=True)
        payload_hash = calculate_sha256(raw_payload)
        source_record = SourceRecord(
            project_id=project_id,
            dataset_name=dataset_name,
            source_table="github_commits",
            source_record_id=sha,
            source_record_hash=payload_hash,
            raw_data=commit_data,
            source_created_at=author_date,
            source_updated_at=commit_date,
            ingestion_batch_id=0,
        )

        # Code changes (files)
        code_changes: List[CodeChange] = []
        files = commit_data.get("files") or []
        for f in files:
            status = f.get("status", "modified")
            status_map = {"added": "A", "modified": "M", "removed": "D", "renamed": "R"}
            change_type = status_map.get(status, "M")
            file_path = f.get("filename") or ""
            additions = int(f.get("additions", 0))
            deletions = int(f.get("deletions", 0))

            code_changes.append(
                CodeChange(
                    commit_hash=sha,
                    file_path=file_path,
                    change_type=change_type,
                    is_deleted=(change_type == "D"),
                    lines_added=additions,
                    lines_removed=deletions,
                )
            )

        # Link commit to Jira issue IDs if detected in commit message
        links: List[CommitIssueLink] = []
        matched_issues = set(cls.ISSUE_PATTERN.findall(message))
        for issue_id in matched_issues:
            links.append(
                CommitIssueLink(
                    commit_hash=sha,
                    source_issue_id=issue_id,
                )
            )

        return commit, source_record, code_changes, links

    @staticmethod
    def normalize_pull_request(
        pr_data: Dict[str, Any],
        project_id: int,
        dataset_name: str,
    ) -> SourceRecord:
        pr_number = str(pr_data.get("number", ""))
        raw_payload = json.dumps(pr_data, sort_keys=True)
        payload_hash = calculate_sha256(raw_payload)

        return SourceRecord(
            project_id=project_id,
            dataset_name=dataset_name,
            source_table="github_pull_requests",
            source_record_id=pr_number,
            source_record_hash=payload_hash,
            raw_data=pr_data,
            source_created_at=parse_github_datetime(pr_data.get("created_at")),
            source_updated_at=parse_github_datetime(pr_data.get("updated_at")),
            ingestion_batch_id=0,
        )
