from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.models.source_connection import SourceConnection
from app.schemas.source_connection import (
    JiraConnectRequest,
    GitHubConnectRequest,
    SourceConnectionResponse,
    ConnectionValidationResponse,
)
from app.schemas.ingestion_run import IngestionRunResponse
from app.services.project_ingestion_service import ProjectIngestionService
from app.integrations.jira.client import JiraCloudClient
from app.integrations.github.client import GitHubClient, parse_github_url

router = APIRouter(prefix="/api/projects/{project_id_or_slug}/sources", tags=["Source Connections"])


# ---------------------------------------------------------------------------
# Jira Cloud
# ---------------------------------------------------------------------------

@router.post("/jira/validate", response_model=ConnectionValidationResponse)
def validate_jira_connection(req: JiraConnectRequest):
    try:
        client = JiraCloudClient(
            base_url=req.base_url,
            email=req.email,
            api_token=req.api_token,
            oauth_token=req.oauth_token,
            timeout=10.0,
        )
        user_info = client.test_connection()
        display_name = user_info.get("displayName", "Authorized User")
        return ConnectionValidationResponse(
            valid=True,
            message=f"Successfully connected to Jira Cloud as '{display_name}'.",
            details={"email": user_info.get("emailAddress"), "account_type": user_info.get("accountType")},
        )
    except Exception as e:
        return ConnectionValidationResponse(
            valid=False,
            message=f"Jira Cloud connection failed: {str(e)}",
        )


@router.post("/jira", response_model=SourceConnectionResponse, status_code=status.HTTP_201_CREATED)
def connect_jira(project_id_or_slug: str, req: JiraConnectRequest, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    conn_name = req.name or f"Jira Cloud ({req.base_url})"
    auth_type = "OAUTH2" if req.oauth_token else ("API_TOKEN" if req.api_token else "NONE")
    config = {
        "base_url": req.base_url,
        "email": req.email,
        "project_key": req.project_key,
    }
    creds = req.oauth_token or req.api_token

    conn = svc.create_source_connection(
        project_id=project.id,
        source_type="JIRA_CLOUD",
        name=conn_name,
        config=config,
        auth_type=auth_type,
        credentials=creds,
    )
    return conn


@router.post("/jira/sync", response_model=IngestionRunResponse)
def sync_jira_source(
    project_id_or_slug: str,
    connection_id: Optional[int] = Query(None),
    jql: Optional[str] = Query(None),
    max_issues: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    if not connection_id:
        # Find default Jira connection for project
        first_conn = (
            db.query(SourceConnection)
            .filter(
                SourceConnection.project_id == project.id,
                SourceConnection.source_type == "JIRA_CLOUD",
            )
            .first()
        )
        if not first_conn:
            raise HTTPException(status_code=400, detail="No Jira Cloud source connected for this project.")
        connection_id = first_conn.id

    run = svc.sync_jira(
        project_id=project.id,
        connection_id=connection_id,
        jql=jql,
        max_issues=max_issues,
    )
    return run


# ---------------------------------------------------------------------------
# GitHub Public Repositories
# ---------------------------------------------------------------------------

@router.post("/github/validate", response_model=ConnectionValidationResponse)
def validate_github_connection(req: GitHubConnectRequest):
    try:
        owner, repo = parse_github_url(req.repo_url)
        client = GitHubClient(token=req.token, timeout=10.0)
        repo_info = client.get_repo(owner=owner, repo=repo)
        full_name = repo_info.get("full_name", f"{owner}/{repo}")
        stars = repo_info.get("stargazers_count", 0)
        return ConnectionValidationResponse(
            valid=True,
            message=f"Successfully validated GitHub public repository '{full_name}' ({stars} stars).",
            details={
                "full_name": full_name,
                "description": repo_info.get("description"),
                "default_branch": repo_info.get("default_branch"),
                "stars": stars,
            },
        )
    except Exception as e:
        return ConnectionValidationResponse(
            valid=False,
            message=f"GitHub connection failed: {str(e)}",
        )


@router.post("/github", response_model=SourceConnectionResponse, status_code=status.HTTP_201_CREATED)
def connect_github(project_id_or_slug: str, req: GitHubConnectRequest, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    try:
        owner, repo = parse_github_url(req.repo_url)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    conn_name = req.name or f"GitHub ({owner}/{repo})"
    auth_type = "API_TOKEN" if req.token else "NONE"
    config = {
        "repo_url": req.repo_url,
        "owner": owner,
        "repo": repo,
    }

    conn = svc.create_source_connection(
        project_id=project.id,
        source_type="GITHUB_PUBLIC",
        name=conn_name,
        config=config,
        auth_type=auth_type,
        credentials=req.token,
    )
    return conn


@router.post("/github/sync", response_model=IngestionRunResponse)
def sync_github_source(
    project_id_or_slug: str,
    connection_id: Optional[int] = Query(None),
    max_commits: int = Query(50, ge=1, le=500),
    max_prs: int = Query(20, ge=0, le=100),
    db: Session = Depends(get_db),
):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    if not connection_id:
        first_conn = (
            db.query(SourceConnection)
            .filter(
                SourceConnection.project_id == project.id,
                SourceConnection.source_type == "GITHUB_PUBLIC",
            )
            .first()
        )
        if not first_conn:
            raise HTTPException(status_code=400, detail="No GitHub source connected for this project.")
        connection_id = first_conn.id

    run = svc.sync_github(
        project_id=project.id,
        connection_id=connection_id,
        max_commits=max_commits,
        max_prs=max_prs,
    )
    return run


# ---------------------------------------------------------------------------
# General Source Connections
# ---------------------------------------------------------------------------

@router.get("", response_model=List[SourceConnectionResponse])
def list_project_sources(project_id_or_slug: str, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    connections = (
        db.query(SourceConnection)
        .filter(SourceConnection.project_id == project.id)
        .order_by(SourceConnection.id.asc())
        .all()
    )
    return connections
