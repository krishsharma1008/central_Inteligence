"""
Core graph models for canonical context graph.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Any, List, Optional
import json


class NodeType(str, Enum):
    """Canonical node types in the context graph."""
    USER = "User"
    CONVERSATION = "Conversation"
    DOCUMENT = "Document"
    ATTACHMENT = "Attachment"
    INTENT = "Intent"
    RULE = "Rule"


class EdgeType(str, Enum):
    """Canonical edge types in the context graph."""
    PART_OF = "PART_OF"
    HAS_ATTACHMENT = "HAS_ATTACHMENT"
    SENT_BY = "SENT_BY"
    SENT_TO = "SENT_TO"
    FOLLOWS = "FOLLOWS"
    MENTIONS = "MENTIONS"
    SEEKS_ANSWER_TO = "SEEKS_ANSWER_TO"
    SELECTED = "SELECTED"


class GraphLayer(str, Enum):
    """Graph layers for stateful memory model."""
    SESSION = "session"
    JOURNEY = "journey"
    PROFILE = "profile"


@dataclass
class Node:
    """
    Canonical graph node with typed properties and governance metadata.
    """
    id: str
    type: NodeType
    props: Dict[str, Any]
    tenant_id: str
    layer: GraphLayer = GraphLayer.SESSION
    ttl_ms: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize node to dictionary."""
        return {
            'id': self.id,
            'type': self.type.value,
            'props': self.props,
            'tenant_id': self.tenant_id,
            'layer': self.layer.value,
            'ttl_ms': self.ttl_ms,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Node':
        """Deserialize node from dictionary."""
        return cls(
            id=data['id'],
            type=NodeType(data['type']),
            props=data.get('props', {}),
            tenant_id=data['tenant_id'],
            layer=GraphLayer(data.get('layer', 'session')),
            ttl_ms=data.get('ttl_ms'),
            created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else None,
            updated_at=datetime.fromisoformat(data['updated_at']) if data.get('updated_at') else None
        )


@dataclass
class Edge:
    """
    Canonical graph edge with typed relationships and state tracking.
    """
    id: str
    src: str
    dst: str
    type: EdgeType
    props: Dict[str, Any]
    tenant_id: str
    state: str = "active"
    layer: GraphLayer = GraphLayer.SESSION
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize edge to dictionary."""
        return {
            'id': self.id,
            'src': self.src,
            'dst': self.dst,
            'type': self.type.value,
            'props': self.props,
            'tenant_id': self.tenant_id,
            'state': self.state,
            'layer': self.layer.value,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Edge':
        """Deserialize edge from dictionary."""
        return cls(
            id=data['id'],
            src=data['src'],
            dst=data['dst'],
            type=EdgeType(data['type']),
            props=data.get('props', {}),
            tenant_id=data['tenant_id'],
            state=data.get('state', 'active'),
            layer=GraphLayer(data.get('layer', 'session')),
            created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else None,
            updated_at=datetime.fromisoformat(data['updated_at']) if data.get('updated_at') else None
        )


@dataclass
class NodeScore:
    """Scoring breakdown for a node during context compilation."""
    node_id: str
    recency_score: float = 0.0
    authority_score: float = 0.0
    stage_score: float = 0.0
    rule_score: float = 0.0
    total_score: float = 0.0
    
    def compute_total(self) -> float:
        """Compute total score from components."""
        self.total_score = (
            self.recency_score + 
            self.authority_score + 
            self.stage_score + 
            self.rule_score
        )
        return self.total_score
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            'node_id': self.node_id,
            'recency_score': self.recency_score,
            'authority_score': self.authority_score,
            'stage_score': self.stage_score,
            'rule_score': self.rule_score,
            'total_score': self.total_score
        }


@dataclass
class CompileTrace:
    """Trace of compilation steps for explainability."""
    request_id: str
    steps: List[Dict[str, Any]] = field(default_factory=list)
    candidate_count: int = 0
    pruned_count: int = 0
    final_count: int = 0
    duration_ms: float = 0.0
    
    def add_step(self, name: str, details: Dict[str, Any]):
        """Add a compilation step."""
        self.steps.append({
            'name': name,
            'timestamp': datetime.utcnow().isoformat(),
            'details': details
        })
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            'request_id': self.request_id,
            'steps': self.steps,
            'candidate_count': self.candidate_count,
            'pruned_count': self.pruned_count,
            'final_count': self.final_count,
            'duration_ms': self.duration_ms
        }


@dataclass
class ContextPacket:
    """
    Compiled context packet with nodes, edges, scores, and lineage.
    This is the deterministic output of the context compiler.
    """
    request_id: str
    intent_node_id: str
    nodes: List[Node]
    edges: List[Edge]
    scores: Dict[str, NodeScore]
    lineage: Dict[str, List[str]]
    trace: CompileTrace
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize context packet to dictionary."""
        return {
            'request_id': self.request_id,
            'intent_node_id': self.intent_node_id,
            'nodes': [n.to_dict() for n in self.nodes],
            'edges': [e.to_dict() for e in self.edges],
            'scores': {k: v.to_dict() for k, v in self.scores.items()},
            'lineage': self.lineage,
            'trace': self.trace.to_dict(),
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
    
    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=2)
    
    def get_node_by_id(self, node_id: str) -> Optional[Node]:
        """Get node by ID."""
        for node in self.nodes:
            if node.id == node_id:
                return node
        return None
    
    def get_edges_for_node(self, node_id: str) -> List[Edge]:
        """Get all edges connected to a node."""
        return [e for e in self.edges if e.src == node_id or e.dst == node_id]
