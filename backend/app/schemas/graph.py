"""
Pydantic Schemas for Work 05: Temporal Knowledge Graph.
"""
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime


class GraphNode(BaseModel):
    id: str = Field(..., description="Unique graph node ID (e.g., 'feature:1', 'issue:KAFKA-100')")
    type: str = Field(..., description="Node type: FEATURE, ISSUE, COMMIT, TEMPORAL_EVENT, CONTRIBUTOR, EVIDENCE")
    label: str = Field(..., description="Display label for visualization")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Metadata, timestamps, attributes")


class GraphEdge(BaseModel):
    id: str = Field(..., description="Unique edge identifier")
    source: str = Field(..., description="Source node ID")
    target: str = Field(..., description="Target node ID")
    type: str = Field(..., description="Relationship type enum")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Edge metadata, confidence, timestamp")


class GraphResponse(BaseModel):
    root_node_id: Optional[str] = None
    node_count: int
    edge_count: int
    nodes: List[GraphNode]
    edges: List[GraphEdge]
    metadata: Dict[str, Any] = Field(default_factory=dict)


class GraphQueryRequest(BaseModel):
    start_node_type: str = Field("FEATURE", description="FEATURE, ISSUE, COMMIT, etc.")
    start_node_id: str = Field(..., description="ID or key of root node (e.g. '1', 'KAFKA-98')")
    depth: int = Field(2, ge=1, le=4, description="Traversal hop depth")
    max_nodes: int = Field(100, ge=1, le=500, description="Max total nodes returned")
    start_time: Optional[datetime] = Field(None, description="Filter elements after this timestamp")
    end_time: Optional[datetime] = Field(None, description="Filter elements before this timestamp")
    node_types: Optional[List[str]] = Field(None, description="Optional node type filter")
    dataset_name: str = Field("KafkaSQLite", description="Dataset scope")
