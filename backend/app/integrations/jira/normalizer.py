"""
Jira Cloud Data Normalizer.
Converts raw Jira v3 JSON objects into normalized domain entities and SourceRecord entries:
- Issues
- Issue Comments
- Issue Components
- Issue Fix Versions
- Issue Links
- Changelog / Status Transitions
"""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.integrations.jira.adf import adf_to_text
from app.models.issue import Issue
from app.models.issue_comment import IssueComment
from app.models.issue_relationships import IssueComponent, IssueFixVersion, IssueLink
from app.models.source_record import SourceRecord


def parse_jira_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse Jira ISO-8601 datetime strings like '2023-01-15T14:32:00.000+0000'."""
    if not dt_str:
        return None
    try:
        # Handle trailing timezone format +0000 -> +00:00
        if len(dt_str) >= 5 and (dt_str[-5] == "+" or dt_str[-5] == "-") and dt_str[-3] != ":":
            dt_str = dt_str[:-2] + ":" + dt_str[-2:]
        return datetime.fromisoformat(dt_str)
    except Exception:
        try:
            return datetime.strptime(dt_str.split(".")[0], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None


def calculate_sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class JiraNormalizer:
    @staticmethod
    def normalize_issue(
        issue_data: Dict[str, Any],
        project_id: int,
        dataset_name: str,
    ) -> Tuple[Issue, SourceRecord, List[IssueComponent], List[IssueFixVersion], List[IssueLink]]:
        """
        Normalize a raw Jira issue dict into an Issue model, a SourceRecord provenance row,
        and associated components, versions, and links.
        """
        key = issue_data.get("key", "")
        fields = issue_data.get("fields", {})

        summary = fields.get("summary") or ""
        desc_node = fields.get("description")
        description = adf_to_text(desc_node) if desc_node else ""

        issue_type_obj = fields.get("issuetype") or {}
        issue_type = issue_type_obj.get("name", "Task") if isinstance(issue_type_obj, dict) else str(issue_type_obj)

        status_obj = fields.get("status") or {}
        status = status_obj.get("name", "Open") if isinstance(status_obj, dict) else str(status_obj)

        priority_obj = fields.get("priority") or {}
        priority = priority_obj.get("name") if isinstance(priority_obj, dict) else None

        resolution_obj = fields.get("resolution") or {}
        resolution = resolution_obj.get("name") if isinstance(resolution_obj, dict) else None

        reporter_obj = fields.get("reporter") or {}
        reporter = reporter_obj.get("displayName") or reporter_obj.get("emailAddress") or reporter_obj.get("accountId")

        assignee_obj = fields.get("assignee") or {}
        assignee = assignee_obj.get("displayName") or assignee_obj.get("emailAddress") or assignee_obj.get("accountId")

        created_at = parse_jira_datetime(fields.get("created"))
        updated_at = parse_jira_datetime(fields.get("updated"))
        resolved_at = parse_jira_datetime(fields.get("resolutiondate"))

        # 1. Issue Domain Model
        issue = Issue(
            source_issue_id=key,
            issue_type=issue_type,
            summary=summary,
            description=description,
            status=status,
            priority=priority,
            resolution=resolution,
            assignee=assignee,
            reporter=reporter,
            created_at=created_at,
            updated_at=updated_at,
            resolved_at=resolved_at,
        )

        # 2. Source Record Provenance
        raw_payload = json.dumps(issue_data, sort_keys=True)
        payload_hash = calculate_sha256(raw_payload)
        source_record = SourceRecord(
            project_id=project_id,
            dataset_name=dataset_name,
            source_table="jira_issues",
            source_record_id=key,
            source_record_hash=payload_hash,
            raw_data=issue_data,
            source_created_at=created_at,
            source_updated_at=updated_at,
            ingestion_batch_id=0,
        )

        # 3. Components
        components: List[IssueComponent] = []
        for c in fields.get("components") or []:
            c_name = c.get("name") if isinstance(c, dict) else str(c)
            if c_name:
                components.append(IssueComponent(
                    source_issue_id=key,
                    component=c_name,
                ))

        # 4. Fix Versions
        fix_versions: List[IssueFixVersion] = []
        for v in fields.get("fixVersions") or []:
            v_name = v.get("name") if isinstance(v, dict) else str(v)
            if v_name:
                fix_versions.append(IssueFixVersion(
                    source_issue_id=key,
                    fix_version=v_name,
                ))

        # 5. Issue Links
        links: List[IssueLink] = []
        for l in fields.get("issuelinks") or []:
            link_type_obj = l.get("type", {})
            if "outwardIssue" in l:
                target_key = l["outwardIssue"].get("key")
                link_type = link_type_obj.get("outward") or link_type_obj.get("name", "relates to")
                if target_key:
                    links.append(IssueLink(
                        source_issue_id=key,
                        target_issue_id=target_key,
                        link_name=link_type,
                        outward_label=link_type,
                        is_containment=False,
                    ))
            elif "inwardIssue" in l:
                target_key = l["inwardIssue"].get("key")
                link_type = link_type_obj.get("inward") or link_type_obj.get("name", "is related to")
                if target_key:
                    links.append(IssueLink(
                        source_issue_id=key,
                        target_issue_id=target_key,
                        link_name=link_type,
                        outward_label=link_type,
                        is_containment=False,
                    ))

        return issue, source_record, components, fix_versions, links

    @staticmethod
    def normalize_comment(
        comment_data: Dict[str, Any],
        issue_key: str,
        project_id: int,
        dataset_name: str,
    ) -> Tuple[IssueComment, SourceRecord]:
        """Normalize a Jira comment dict."""
        comment_id = str(comment_data.get("id", ""))
        body_node = comment_data.get("body")
        body = adf_to_text(body_node) if body_node else ""

        author_obj = comment_data.get("author") or {}
        author = author_obj.get("displayName") or author_obj.get("emailAddress") or author_obj.get("accountId") or "Unknown"

        created_at = parse_jira_datetime(comment_data.get("created"))
        updated_at = parse_jira_datetime(comment_data.get("updated"))

        comment = IssueComment(
            source_issue_id=issue_key,
            username=author,
            display_name=author,
            message=body,
            created_at=created_at,
        )

        raw_payload = json.dumps(comment_data, sort_keys=True)
        payload_hash = calculate_sha256(raw_payload)
        source_record = SourceRecord(
            project_id=project_id,
            dataset_name=dataset_name,
            source_table="jira_comments",
            source_record_id=comment_id,
            source_record_hash=payload_hash,
            raw_data=comment_data,
            source_created_at=created_at,
            source_updated_at=updated_at,
            ingestion_batch_id=0,
        )

        return comment, source_record

    @staticmethod
    def normalize_changelog_entries(
        issue_key: str,
        changelog_data: Dict[str, Any],
        project_id: int,
        dataset_name: str,
    ) -> List[SourceRecord]:
        """
        Normalize issue changelog histories (e.g. status transitions, assignee changes)
        into SourceRecord audit entries for temporal event extraction.
        """
        histories = changelog_data.get("histories", []) if isinstance(changelog_data, dict) else changelog_data
        records = []

        for h in histories:
            h_id = str(h.get("id", ""))
            created_at = parse_jira_datetime(h.get("created"))

            raw_payload = json.dumps(h, sort_keys=True)
            payload_hash = calculate_sha256(raw_payload)

            source_record = SourceRecord(
                project_id=project_id,
                dataset_name=dataset_name,
                source_table="jira_changelogs",
                source_record_id=f"{issue_key}-hist-{h_id}",
                source_record_hash=payload_hash,
                raw_data=h,
                source_created_at=created_at,
                source_updated_at=created_at,
                ingestion_batch_id=0,
            )
            records.append(source_record)

        return records
