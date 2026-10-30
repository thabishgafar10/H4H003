"""
FastAPI router for Work 05: Temporal Knowledge Graph.
Provides graph traversal, neighborhood queries, and topological metrics.
"""
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas.graph import GraphResponse, GraphQueryRequest
from app.services.knowledge_graph_service import KnowledgeGraphService

router = APIRouter(prefix="/api/graph", tags=["Knowledge Graph"])


@router.get("/features/{feature_id}", response_model=GraphResponse)
def get_feature_graph(
    feature_id: int,
    depth: int = Query(2, ge=1, le=4),
    max_nodes: int = Query(80, ge=5, le=300),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    dataset_name: str = Query("KafkaSQLite"),
    project_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """Returns the multi-hop temporal knowledge graph centered on a canonical feature."""
    service = KnowledgeGraphService(db, dataset_name=dataset_name, project_id=project_id)
    graph = service.get_feature_graph(
        feature_id=feature_id,
        depth=depth,
        max_nodes=max_nodes,
        start_time=start_time,
        end_time=end_time,
    )
    if graph.node_count == 0:
        raise HTTPException(status_code=404, detail=f"Feature with ID {feature_id} not found in graph.")
    return graph


@router.get("/nodes/{node_type}/{node_id}", response_model=GraphResponse)
def get_node_subgraph(
    node_type: str,
    node_id: str,
    depth: int = Query(2, ge=1, le=4),
    max_nodes: int = Query(80, ge=5, le=300),
    dataset_name: str = Query("KafkaSQLite"),
    project_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """Returns the neighborhood subgraph centered on an arbitrary node (feature, issue, commit)."""
    service = KnowledgeGraphService(db, dataset_name=dataset_name, project_id=project_id)
    req = GraphQueryRequest(
        start_node_type=node_type.upper(),
        start_node_id=node_id,
        depth=depth,
        max_nodes=max_nodes,
        dataset_name=dataset_name,
        project_id=project_id,
    )
    graph = service.query_graph(req)
    if graph.node_count == 0:
        raise HTTPException(status_code=404, detail=f"Node {node_type}:{node_id} not found in graph.")
    return graph


@router.post("/query", response_model=GraphResponse)
def query_graph(
    request: GraphQueryRequest,
    db: Session = Depends(get_db),
):
    """Executes a custom parameterized graph traversal with filters and bounds."""
    service = KnowledgeGraphService(db, dataset_name=request.dataset_name, project_id=request.project_id)
    return service.query_graph(request)


@router.get("/stats")
def get_graph_stats(
    dataset_name: str = Query("KafkaSQLite"),
    db: Session = Depends(get_db),
):
    """Returns high-level graph topology metrics and entity distribution counts."""
    service = KnowledgeGraphService(db, dataset_name=dataset_name)
    return service.get_graph_statistics()

