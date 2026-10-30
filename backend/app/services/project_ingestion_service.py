"""
Project Ingestion Orchestration Service for Work 07.
Coordinates multi-source data ingestion for projects (Jira Cloud, GitHub Public, Documents),
maintains IngestionRun execution state, incremental sync checkpoints, and idempotency.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
import httpx
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.project import Project
from app.models.source_connection import SourceConnection
from app.models.document import Document
from app.models.ingestion_run import IngestionRun
from app.models.source_record import SourceRecord
from app.models.issue import Issue
from app.models.issue_comment import IssueComment
from app.models.commit import Commit
from app.models.code_change import CodeChange
from app.models.issue_relationships import IssueComponent, IssueFixVersion, IssueLink, CommitIssueLink

from app.integrations.jira.client import JiraCloudClient
from app.integrations.jira.normalizer import JiraNormalizer
from app.integrations.github.client import GitHubClient, parse_github_url
from app.integrations.github.normalizer import GitHubNormalizer
from app.integrations.documents.security import validate_public_url, sanitize_filename
from app.integrations.documents.normalizer import DocumentNormalizer

logger = logging.getLogger(__name__)


def slugify(text: str) -> str:
    cleaned = re.sub(r"[^\w\s-]", "", text.lower()).strip()
    return re.sub(r"[-\s]+", "-", cleaned)


class ProjectIngestionService:
    def __init__(self, db: Session):
        self.db = db

    # -------------------------------------------------------------------------
    # Project Management
    # -------------------------------------------------------------------------

    def create_project(
        self,
        name: str,
        slug: Optional[str] = None,
        description: Optional[str] = None,
        domain: Optional[str] = None,
    ) -> Project:
        if not name or not name.strip():
            raise ValueError("Project name cannot be empty.")

        target_slug = slugify(slug or name)
        existing = self.db.query(Project).filter(Project.slug == target_slug).first()
        if existing:
            raise ValueError(f"Project with slug '{target_slug}' already exists.")

        project = Project(
            name=name.strip(),
            slug=target_slug,
            description=description.strip() if description else None,
            domain=domain.strip() if domain else None,
            status="READY",
        )
        self.db.add(project)
        self.db.commit()
        self.db.refresh(project)
        return project

    def get_project(self, project_id_or_slug: Union[int, str]) -> Optional[Project]:
        if isinstance(project_id_or_slug, int) or (isinstance(project_id_or_slug, str) and project_id_or_slug.isdigit()):
            return self.db.query(Project).filter(Project.id == int(project_id_or_slug)).first()
        return self.db.query(Project).filter(Project.slug == str(project_id_or_slug)).first()

    def list_projects(self) -> List[Project]:
        return self.db.query(Project).order_by(Project.id.asc()).all()

    # -------------------------------------------------------------------------
    # Source Connection Management
    # -------------------------------------------------------------------------

    def create_source_connection(
        self,
        project_id: int,
        source_type: str,
        name: str,
        config: Dict[str, Any],
        auth_type: str = "NONE",
        credentials: Optional[str] = None,
    ) -> SourceConnection:
        project = self.get_project(project_id)
        if not project:
            raise ValueError(f"Project {project_id} not found.")

        conn = SourceConnection(
            project_id=project.id,
            source_type=source_type.upper(),
            name=name.strip(),
            config=json.dumps(config),
            auth_type=auth_type.upper(),
            credentials_encrypted=credentials,
            status="CONNECTED",
        )
        self.db.add(conn)
        self.db.commit()
        self.db.refresh(conn)
        return conn

    # -------------------------------------------------------------------------
    # Ingestion Run Lifecycle
    # -------------------------------------------------------------------------

    def create_ingestion_run(
        self,
        project_id: int,
        source_system: str,
        source_connection_id: Optional[int] = None,
    ) -> IngestionRun:
        run = IngestionRun(
            project_id=project_id,
            source_connection_id=source_connection_id,
            source_system=source_system,
            status="RUNNING",
            started_at=datetime.now(timezone.utc),
            records_discovered=0,
            records_created=0,
            records_updated=0,
            records_skipped=0,
            records_failed=0,
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def complete_ingestion_run(
        self,
        run: IngestionRun,
        checkpoint: Optional[Dict[str, Any]] = None,
        status: str = "COMPLETED",
        error_summary: Optional[str] = None,
    ) -> IngestionRun:
        run.status = status
        run.completed_at = datetime.now(timezone.utc)
        if checkpoint:
            run.checkpoint_data = json.dumps(checkpoint)
        if error_summary:
            run.error_summary = error_summary
        self.db.commit()
        self.db.refresh(run)
        return run

    # -------------------------------------------------------------------------
    # Jira Cloud Sync
    # -------------------------------------------------------------------------

    def sync_jira(
        self,
        project_id: int,
        connection_id: Optional[int] = None,
        client: Optional[JiraCloudClient] = None,
        jql: Optional[str] = None,
        max_issues: int = 50,
    ) -> IngestionRun:
        project = self.get_project(project_id)
        if not project:
            raise ValueError(f"Project {project_id} not found.")

        run = self.create_ingestion_run(
            project_id=project.id,
            source_system="JIRA_CLOUD",
            source_connection_id=connection_id,
        )

        try:
            # Resolve client from connection if not injected
            if client is None:
                if not connection_id:
                    raise ValueError("connection_id or initialized client required for Jira sync.")
                conn = self.db.query(SourceConnection).filter(SourceConnection.id == connection_id).first()
                if not conn:
                    raise ValueError(f"Source connection {connection_id} not found.")
                config = json.loads(conn.config)
                base_url = config.get("base_url") or config.get("url")
                email = config.get("email")
                api_token = conn.credentials_encrypted or config.get("api_token")
                client = JiraCloudClient(base_url=base_url, email=email, api_token=api_token)

            # Determine checkpoint from last successful run
            last_run = (
                self.db.query(IngestionRun)
                .filter(
                    IngestionRun.project_id == project.id,
                    IngestionRun.source_system == "JIRA_CLOUD",
                    IngestionRun.status == "COMPLETED",
                    IngestionRun.checkpoint_data.isnot(None),
                )
                .order_by(IngestionRun.id.desc())
                .first()
            )
            checkpoint = json.loads(last_run.checkpoint_data) if last_run and last_run.checkpoint_data else {}
            last_sync_time = checkpoint.get("last_updated")

            effective_jql = jql
            if not effective_jql:
                effective_jql = "ORDER BY updated ASC"
                if last_sync_time:
                    effective_jql = f"updated >= '{last_sync_time}' {effective_jql}"

            # Fetch issues
            search_res = client.search_issues(jql=effective_jql, max_results=max_issues)
            issues_data = search_res.get("issues", [])
            run.records_discovered = len(issues_data)

            latest_seen_updated = last_sync_time

            for issue_data in issues_data:
                try:
                    norm_issue, src_rec, comps, fixes, links = JiraNormalizer.normalize_issue(
                        issue_data=issue_data,
                        project_id=project.id,
                        dataset_name=project.slug,
                    )

                    # Check idempotency via SourceRecord
                    existing_rec = (
                        self.db.query(SourceRecord)
                        .filter(
                            SourceRecord.project_id == project.id,
                            SourceRecord.source_table == "jira_issues",
                            SourceRecord.source_record_id == norm_issue.source_issue_id,
                        )
                        .first()
                    )

                    if existing_rec:
                        if existing_rec.source_record_hash == src_rec.source_record_hash:
                            run.records_skipped += 1
                        else:
                            existing_rec.source_record_hash = src_rec.source_record_hash
                            existing_rec.raw_data = src_rec.raw_data
                            existing_rec.source_updated_at = src_rec.source_updated_at

                            d_issue = (
                                self.db.query(Issue)
                                .filter(Issue.source_issue_id == norm_issue.source_issue_id)
                                .first()
                            )
                            if d_issue:
                                d_issue.summary = norm_issue.summary
                                d_issue.description = norm_issue.description
                                d_issue.status = norm_issue.status
                                d_issue.updated_at = norm_issue.updated_at
                            run.records_updated += 1
                    else:
                        self.db.add(src_rec)
                        self.db.flush()
                        norm_issue.source_record_id = src_rec.id
                        self.db.add(norm_issue)
                        for c in comps:
                            c.source_record_id = src_rec.id
                            self.db.add(c)
                        for f in fixes:
                            f.source_record_id = src_rec.id
                            self.db.add(f)
                        for l in links:
                            l.source_record_id = src_rec.id
                            self.db.add(l)
                        run.records_created += 1

                    # Comments
                    fields = issue_data.get("fields", {})
                    comment_block = fields.get("comment", {})
                    for c_data in comment_block.get("comments", []):
                        norm_c, c_rec = JiraNormalizer.normalize_comment(
                            comment_data=c_data,
                            issue_key=norm_issue.source_issue_id,
                            project_id=project.id,
                            dataset_name=project.slug,
                        )
                        c_exist = (
                            self.db.query(SourceRecord)
                            .filter(
                                SourceRecord.project_id == project.id,
                                SourceRecord.source_table == "jira_comments",
                                SourceRecord.source_record_id == str(c_data.get("id")),
                            )
                            .first()
                        )
                        if not c_exist:
                            self.db.add(c_rec)
                            self.db.flush()
                            norm_c.source_record_id = c_rec.id
                            self.db.add(norm_c)

                    # Changelog
                    if "changelog" in issue_data:
                        ch_records = JiraNormalizer.normalize_changelog_entries(
                            issue_key=norm_issue.source_issue_id,
                            changelog_data=issue_data["changelog"],
                            project_id=project.id,
                            dataset_name=project.slug,
                        )
                        for ch_rec in ch_records:
                            ch_exist = (
                                self.db.query(SourceRecord)
                                .filter(
                                    SourceRecord.project_id == project.id,
                                    SourceRecord.source_table == "jira_changelogs",
                                    SourceRecord.source_record_id == ch_rec.source_record_id,
                                )
                                .first()
                            )
                            if not ch_exist:
                                self.db.add(ch_rec)

                    up_str = fields.get("updated")
                    if up_str and (not latest_seen_updated or up_str > latest_seen_updated):
                        latest_seen_updated = up_str

                    self.db.commit()

                except Exception as row_exc:
                    logger.error("Error normalizing issue %s: %s", issue_data.get("key"), row_exc)
                    self.db.rollback()
                    run.records_failed += 1

            new_checkpoint = {"last_updated": latest_seen_updated}
            return self.complete_ingestion_run(run, checkpoint=new_checkpoint, status="COMPLETED")

        except Exception as e:
            logger.exception("Jira sync failed for project %s: %s", project_id, e)
            self.db.rollback()
            return self.complete_ingestion_run(run, status="FAILED", error_summary=str(e))

    # -------------------------------------------------------------------------
    # GitHub Public Repository Sync
    # -------------------------------------------------------------------------

    def sync_github(
        self,
        project_id: int,
        connection_id: Optional[int] = None,
        client: Optional[GitHubClient] = None,
        repo_url_or_slug: Optional[str] = None,
        max_commits: int = 50,
        max_prs: int = 20,
    ) -> IngestionRun:
        project = self.get_project(project_id)
        if not project:
            raise ValueError(f"Project {project_id} not found.")

        run = self.create_ingestion_run(
            project_id=project.id,
            source_system="GITHUB_PUBLIC",
            source_connection_id=connection_id,
        )

        try:
            token = None
            if not repo_url_or_slug:
                if not connection_id:
                    raise ValueError("connection_id or repo_url_or_slug required for GitHub sync.")
                conn = self.db.query(SourceConnection).filter(SourceConnection.id == connection_id).first()
                if not conn:
                    raise ValueError(f"Source connection {connection_id} not found.")
                config = json.loads(conn.config)
                repo_url_or_slug = config.get("repo_url") or config.get("repository")
                token = conn.credentials_encrypted or config.get("token")

            owner, repo = parse_github_url(repo_url_or_slug)
            if client is None:
                client = GitHubClient(token=token)

            last_run = (
                self.db.query(IngestionRun)
                .filter(
                    IngestionRun.project_id == project.id,
                    IngestionRun.source_system == "GITHUB_PUBLIC",
                    IngestionRun.status == "COMPLETED",
                    IngestionRun.checkpoint_data.isnot(None),
                )
                .order_by(IngestionRun.id.desc())
                .first()
            )
            checkpoint = json.loads(last_run.checkpoint_data) if last_run and last_run.checkpoint_data else {}
            last_commit_sha = checkpoint.get("last_commit_sha")

            # 1. Commits Sync
            commits_list = client.list_commits(owner=owner, repo=repo, per_page=min(max_commits, 100))
            run.records_discovered += len(commits_list)

            latest_sha = commits_list[0].get("sha") if commits_list else last_commit_sha

            for c_summary in commits_list:
                sha = c_summary.get("sha")
                if sha == last_commit_sha:
                    break

                try:
                    existing_rec = (
                        self.db.query(SourceRecord)
                        .filter(
                            SourceRecord.project_id == project.id,
                            SourceRecord.source_table == "github_commits",
                            SourceRecord.source_record_id == sha,
                        )
                        .first()
                    )

                    if existing_rec:
                        run.records_skipped += 1
                        continue

                    commit_detail = client.get_commit(owner=owner, repo=repo, ref=sha)
                    norm_commit, src_rec, code_changes, links = GitHubNormalizer.normalize_commit(
                        commit_data=commit_detail,
                        project_id=project.id,
                        dataset_name=project.slug,
                    )

                    self.db.add(src_rec)
                    self.db.flush()
                    norm_commit.source_record_id = src_rec.id
                    self.db.add(norm_commit)
                    for change in code_changes:
                        change.source_record_id = src_rec.id
                        self.db.add(change)
                    for l in links:
                        l.source_record_id = src_rec.id
                        self.db.add(l)

                    self.db.commit()
                    run.records_created += 1

                except Exception as commit_exc:
                    logger.error("Error processing GitHub commit %s: %s", sha, commit_exc)
                    self.db.rollback()
                    run.records_failed += 1

            # 2. Pull Requests Sync
            if max_prs > 0:
                try:
                    prs_list = client.list_pull_requests(owner=owner, repo=repo, per_page=min(max_prs, 100))
                    run.records_discovered += len(prs_list)
                    for pr in prs_list:
                        pr_num = str(pr.get("number"))
                        pr_exist = (
                            self.db.query(SourceRecord)
                            .filter(
                                SourceRecord.project_id == project.id,
                                SourceRecord.source_table == "github_pull_requests",
                                SourceRecord.source_record_id == pr_num,
                            )
                            .first()
                        )
                        if pr_exist:
                            run.records_skipped += 1
                            continue

                        pr_rec = GitHubNormalizer.normalize_pull_request(
                            pr_data=pr,
                            project_id=project.id,
                            dataset_name=project.slug,
                        )
                        self.db.add(pr_rec)
                        self.db.commit()
                        run.records_created += 1

                except Exception as pr_exc:
                    logger.warning("Error fetching pull requests for %s/%s: %s", owner, repo, pr_exc)
                    self.db.rollback()

            new_checkpoint = {"last_commit_sha": latest_sha}
            return self.complete_ingestion_run(run, checkpoint=new_checkpoint, status="COMPLETED")

        except Exception as e:
            logger.exception("GitHub sync failed for project %s: %s", project_id, e)
            self.db.rollback()
            return self.complete_ingestion_run(run, status="FAILED", error_summary=str(e))

    # -------------------------------------------------------------------------
    # Document Ingestion (File Upload & Safe URL)
    # -------------------------------------------------------------------------

    def ingest_document_bytes(
        self,
        project_id: int,
        filename: str,
        file_bytes: bytes,
        source_connection_id: Optional[int] = None,
        source_url: Optional[str] = None,
    ) -> Tuple[Document, IngestionRun]:
        project = self.get_project(project_id)
        if not project:
            raise ValueError(f"Project {project_id} not found.")

        system_type = "DOCUMENT_URL" if source_url else "DOCUMENT_UPLOAD"
        run = self.create_ingestion_run(
            project_id=project.id,
            source_system=system_type,
            source_connection_id=source_connection_id,
        )

        try:
            doc_model, src_records = DocumentNormalizer.process_file_bytes(
                filename=filename,
                file_bytes=file_bytes,
                project_id=project.id,
                dataset_name=project.slug,
                source_connection_id=source_connection_id,
                source_url=source_url,
            )

            existing_doc = (
                self.db.query(Document)
                .filter(
                    Document.project_id == project.id,
                    Document.sha256_hash == doc_model.sha256_hash,
                )
                .first()
            )

            if existing_doc:
                run.records_discovered = 1
                run.records_skipped = 1
                return existing_doc, self.complete_ingestion_run(run, status="COMPLETED")

            self.db.add(doc_model)
            for rec in src_records:
                self.db.add(rec)

            self.db.commit()
            self.db.refresh(doc_model)

            run.records_discovered = 1 + len(src_records)
            run.records_created = 1 + len(src_records)
            return doc_model, self.complete_ingestion_run(run, status="COMPLETED")

        except Exception as e:
            logger.exception("Document ingestion failed for %s: %s", filename, e)
            self.db.rollback()
            return None, self.complete_ingestion_run(run, status="FAILED", error_summary=str(e))

    def ingest_document_url(
        self,
        project_id: int,
        url: str,
        source_connection_id: Optional[int] = None,
    ) -> Tuple[Document, IngestionRun]:
        validate_public_url(url)

        with httpx.Client(timeout=20.0, follow_redirects=False) as client:
            resp = client.get(url)
            resp.raise_for_status()
            content = resp.content

        from urllib.parse import urlparse
        path = urlparse(url).path
        filename = sanitize_filename(path.split("/")[-1] or "document.txt")

        return self.ingest_document_bytes(
            project_id=project_id,
            filename=filename,
            file_bytes=content,
            source_connection_id=source_connection_id,
            source_url=url,
        )
