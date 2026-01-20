"""
SQLite-based graph store for canonical context graph.
"""

import sqlite3
import json
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from .models import Node, Edge, NodeType, EdgeType, ContextPacket, GraphLayer

logger = logging.getLogger('outlook-email.context_graph.store')


class GraphStoreSQLite:
    """
    SQLite storage adapter for the canonical context graph.
    Stores nodes, edges, and context packets with governance metadata.
    """
    
    def __init__(self, db_path: str):
        """
        Initialize graph store.
        
        Args:
            db_path: Path to SQLite database (same as email database)
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        logger.info("GraphStoreSQLite initialized")
    
    def _create_tables(self):
        """Create graph tables if they don't exist."""
        cursor = self.conn.cursor()
        
        # Graph nodes table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS graph_nodes (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            props TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            layer TEXT DEFAULT 'session',
            ttl_ms INTEGER,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL
        )
        ''')
        
        # Graph edges table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS graph_edges (
            id TEXT PRIMARY KEY,
            src TEXT NOT NULL,
            dst TEXT NOT NULL,
            type TEXT NOT NULL,
            props TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            state TEXT DEFAULT 'active',
            layer TEXT DEFAULT 'session',
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            FOREIGN KEY (src) REFERENCES graph_nodes(id),
            FOREIGN KEY (dst) REFERENCES graph_nodes(id)
        )
        ''')
        
        # Context packets table for explainability
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS context_packets (
            request_id TEXT PRIMARY KEY,
            intent_node_id TEXT NOT NULL,
            nodes TEXT NOT NULL,
            edges TEXT NOT NULL,
            scores TEXT NOT NULL,
            lineage TEXT NOT NULL,
            trace TEXT NOT NULL,
            created_at DATETIME NOT NULL
        )
        ''')
        
        # Create indices
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_nodes_type ON graph_nodes(type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_nodes_tenant ON graph_nodes(tenant_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_nodes_layer ON graph_nodes(layer)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_edges_src ON graph_edges(src)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_edges_dst ON graph_edges(dst)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_edges_type ON graph_edges(type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_edges_tenant ON graph_edges(tenant_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_edges_state ON graph_edges(state)')
        except sqlite3.OperationalError:
            pass
        
        self.conn.commit()
        logger.info("Graph tables created/verified")
    
    def add_node(self, node: Node) -> bool:
        """
        Add a node to the graph.
        
        Args:
            node: Node to add
            
        Returns:
            bool: True if successful
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
            INSERT OR REPLACE INTO graph_nodes 
            (id, type, props, tenant_id, layer, ttl_ms, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                node.id,
                node.type.value,
                json.dumps(node.props),
                node.tenant_id,
                node.layer.value,
                node.ttl_ms,
                node.created_at.isoformat() if node.created_at else datetime.utcnow().isoformat(),
                node.updated_at.isoformat() if node.updated_at else datetime.utcnow().isoformat()
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding node {node.id}: {str(e)}")
            self.conn.rollback()
            return False
    
    def add_edge(self, edge: Edge) -> bool:
        """
        Add an edge to the graph.
        
        Args:
            edge: Edge to add
            
        Returns:
            bool: True if successful
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
            INSERT OR REPLACE INTO graph_edges 
            (id, src, dst, type, props, tenant_id, state, layer, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                edge.id,
                edge.src,
                edge.dst,
                edge.type.value,
                json.dumps(edge.props),
                edge.tenant_id,
                edge.state,
                edge.layer.value,
                edge.created_at.isoformat() if edge.created_at else datetime.utcnow().isoformat(),
                edge.updated_at.isoformat() if edge.updated_at else datetime.utcnow().isoformat()
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding edge {edge.id}: {str(e)}")
            self.conn.rollback()
            return False
    
    def get_node(self, node_id: str) -> Optional[Node]:
        """
        Get a node by ID.
        
        Args:
            node_id: Node ID
            
        Returns:
            Node if found, None otherwise
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM graph_nodes WHERE id = ?', (node_id,))
            row = cursor.fetchone()
            if row:
                return Node(
                    id=row['id'],
                    type=NodeType(row['type']),
                    props=json.loads(row['props']),
                    tenant_id=row['tenant_id'],
                    layer=GraphLayer(row['layer']),
                    ttl_ms=row['ttl_ms'],
                    created_at=datetime.fromisoformat(row['created_at']),
                    updated_at=datetime.fromisoformat(row['updated_at'])
                )
            return None
        except Exception as e:
            logger.error(f"Error getting node {node_id}: {str(e)}")
            return None
    
    def get_nodes_by_type(self, node_type: NodeType, tenant_id: Optional[str] = None) -> List[Node]:
        """
        Get all nodes of a specific type.
        
        Args:
            node_type: Type of nodes to retrieve
            tenant_id: Optional tenant filter
            
        Returns:
            List of nodes
        """
        try:
            cursor = self.conn.cursor()
            if tenant_id:
                cursor.execute(
                    'SELECT * FROM graph_nodes WHERE type = ? AND tenant_id = ?',
                    (node_type.value, tenant_id)
                )
            else:
                cursor.execute('SELECT * FROM graph_nodes WHERE type = ?', (node_type.value,))
            
            nodes = []
            for row in cursor.fetchall():
                nodes.append(Node(
                    id=row['id'],
                    type=NodeType(row['type']),
                    props=json.loads(row['props']),
                    tenant_id=row['tenant_id'],
                    layer=GraphLayer(row['layer']),
                    ttl_ms=row['ttl_ms'],
                    created_at=datetime.fromisoformat(row['created_at']),
                    updated_at=datetime.fromisoformat(row['updated_at'])
                ))
            return nodes
        except Exception as e:
            logger.error(f"Error getting nodes by type {node_type}: {str(e)}")
            return []
    
    def get_edges_for_node(self, node_id: str, edge_type: Optional[EdgeType] = None) -> List[Edge]:
        """
        Get all edges connected to a node.
        
        Args:
            node_id: Node ID
            edge_type: Optional edge type filter
            
        Returns:
            List of edges
        """
        try:
            cursor = self.conn.cursor()
            if edge_type:
                cursor.execute(
                    'SELECT * FROM graph_edges WHERE (src = ? OR dst = ?) AND type = ? AND state = ?',
                    (node_id, node_id, edge_type.value, 'active')
                )
            else:
                cursor.execute(
                    'SELECT * FROM graph_edges WHERE (src = ? OR dst = ?) AND state = ?',
                    (node_id, node_id, 'active')
                )
            
            edges = []
            for row in cursor.fetchall():
                edges.append(Edge(
                    id=row['id'],
                    src=row['src'],
                    dst=row['dst'],
                    type=EdgeType(row['type']),
                    props=json.loads(row['props']),
                    tenant_id=row['tenant_id'],
                    state=row['state'],
                    layer=GraphLayer(row['layer']),
                    created_at=datetime.fromisoformat(row['created_at']),
                    updated_at=datetime.fromisoformat(row['updated_at'])
                ))
            return edges
        except Exception as e:
            logger.error(f"Error getting edges for node {node_id}: {str(e)}")
            return []
    
    def get_outgoing_edges(self, node_id: str, edge_type: Optional[EdgeType] = None) -> List[Edge]:
        """Get outgoing edges from a node."""
        try:
            cursor = self.conn.cursor()
            if edge_type:
                cursor.execute(
                    'SELECT * FROM graph_edges WHERE src = ? AND type = ? AND state = ?',
                    (node_id, edge_type.value, 'active')
                )
            else:
                cursor.execute(
                    'SELECT * FROM graph_edges WHERE src = ? AND state = ?',
                    (node_id, 'active')
                )
            
            edges = []
            for row in cursor.fetchall():
                edges.append(Edge(
                    id=row['id'],
                    src=row['src'],
                    dst=row['dst'],
                    type=EdgeType(row['type']),
                    props=json.loads(row['props']),
                    tenant_id=row['tenant_id'],
                    state=row['state'],
                    layer=GraphLayer(row['layer']),
                    created_at=datetime.fromisoformat(row['created_at']),
                    updated_at=datetime.fromisoformat(row['updated_at'])
                ))
            return edges
        except Exception as e:
            logger.error(f"Error getting outgoing edges for node {node_id}: {str(e)}")
            return []
    
    def save_context_packet(self, packet: ContextPacket) -> bool:
        """
        Save a context packet for explainability.
        
        Args:
            packet: Context packet to save
            
        Returns:
            bool: True if successful
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
            INSERT OR REPLACE INTO context_packets 
            (request_id, intent_node_id, nodes, edges, scores, lineage, trace, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                packet.request_id,
                packet.intent_node_id,
                json.dumps([n.to_dict() for n in packet.nodes]),
                json.dumps([e.to_dict() for e in packet.edges]),
                json.dumps({k: v.to_dict() for k, v in packet.scores.items()}),
                json.dumps(packet.lineage),
                json.dumps(packet.trace.to_dict()),
                packet.created_at.isoformat() if packet.created_at else datetime.utcnow().isoformat()
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error saving context packet {packet.request_id}: {str(e)}")
            self.conn.rollback()
            return False
    
    def get_context_packet(self, request_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a saved context packet.
        
        Args:
            request_id: Request ID
            
        Returns:
            Context packet data if found
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM context_packets WHERE request_id = ?', (request_id,))
            row = cursor.fetchone()
            if row:
                return {
                    'request_id': row['request_id'],
                    'intent_node_id': row['intent_node_id'],
                    'nodes': json.loads(row['nodes']),
                    'edges': json.loads(row['edges']),
                    'scores': json.loads(row['scores']),
                    'lineage': json.loads(row['lineage']),
                    'trace': json.loads(row['trace']),
                    'created_at': row['created_at']
                }
            return None
        except Exception as e:
            logger.error(f"Error getting context packet {request_id}: {str(e)}")
            return None
    
    def get_graph_stats(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get graph statistics.
        
        Args:
            tenant_id: Optional tenant filter
            
        Returns:
            Dictionary with counts by type
        """
        try:
            cursor = self.conn.cursor()
            stats = {}
            
            # Node counts by type
            if tenant_id:
                cursor.execute(
                    'SELECT type, COUNT(*) as count FROM graph_nodes WHERE tenant_id = ? GROUP BY type',
                    (tenant_id,)
                )
            else:
                cursor.execute('SELECT type, COUNT(*) as count FROM graph_nodes GROUP BY type')
            
            stats['nodes_by_type'] = {row['type']: row['count'] for row in cursor.fetchall()}
            
            # Edge counts by type
            if tenant_id:
                cursor.execute(
                    'SELECT type, COUNT(*) as count FROM graph_edges WHERE tenant_id = ? AND state = ? GROUP BY type',
                    (tenant_id, 'active')
                )
            else:
                cursor.execute(
                    'SELECT type, COUNT(*) as count FROM graph_edges WHERE state = ? GROUP BY type',
                    ('active',)
                )
            
            stats['edges_by_type'] = {row['type']: row['count'] for row in cursor.fetchall()}
            
            # Total counts
            if tenant_id:
                cursor.execute('SELECT COUNT(*) FROM graph_nodes WHERE tenant_id = ?', (tenant_id,))
            else:
                cursor.execute('SELECT COUNT(*) FROM graph_nodes')
            stats['total_nodes'] = cursor.fetchone()[0]
            
            if tenant_id:
                cursor.execute('SELECT COUNT(*) FROM graph_edges WHERE tenant_id = ? AND state = ?', (tenant_id, 'active'))
            else:
                cursor.execute('SELECT COUNT(*) FROM graph_edges WHERE state = ?', ('active',))
            stats['total_edges'] = cursor.fetchone()[0]
            
            return stats
        except Exception as e:
            logger.error(f"Error getting graph stats: {str(e)}")
            return {}
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
