"""
Context compiler for deterministic graph-based context resolution.
"""

import logging
import time
import math
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timedelta
from collections import defaultdict
import uuid

from .models import (
    Node, Edge, NodeType, EdgeType, ContextPacket, 
    NodeScore, CompileTrace, GraphLayer
)
from .store_sqlite import GraphStoreSQLite
from .vector_scorer import VectorScorer

logger = logging.getLogger('outlook-email.context_graph.compiler')


class ContextCompiler:
    """
    Deterministic context compiler that produces bounded, high-signal working contexts
    by traversing and scoring the graph.
    """
    
    def __init__(
        self,
        graph_store: GraphStoreSQLite,
        vector_scorer: Optional[VectorScorer] = None,
        recency_half_life_days: float = 7.0,
        max_nodes: int = 50,
        max_edges: int = 100,
        max_tokens: int = 8000
    ):
        """
        Initialize context compiler.
        
        Args:
            graph_store: Graph storage backend
            vector_scorer: Optional vector scorer for semantic similarity
            recency_half_life_days: Half-life for recency decay (exponential)
            max_nodes: Maximum nodes in compiled context
            max_edges: Maximum edges in compiled context
            max_tokens: Estimated token budget for context
        """
        self.graph_store = graph_store
        self.vector_scorer = vector_scorer
        self.recency_half_life_days = recency_half_life_days
        self.max_nodes = max_nodes
        self.max_edges = max_edges
        self.max_tokens = max_tokens
    
    def compile(
        self,
        intent_text: str,
        seed_node_ids: List[str],
        tenant_id: str,
        request_id: Optional[str] = None,
        debug: bool = False
    ) -> ContextPacket:
        """
        Compile a context packet from seed nodes.
        
        Args:
            intent_text: User's question/intent
            seed_node_ids: Seed nodes (e.g., conversation IDs from FTS/vector search)
            tenant_id: Tenant ID for governance
            request_id: Optional request ID for tracking
            debug: Enable debug mode for detailed traces
            
        Returns:
            ContextPacket with selected nodes, edges, scores, and trace
        """
        start_time = time.time()
        request_id = request_id or str(uuid.uuid4())
        
        trace = CompileTrace(request_id=request_id)
        trace.add_step('init', {
            'intent': intent_text,
            'seed_count': len(seed_node_ids),
            'tenant_id': tenant_id
        })
        
        # Step 1: Create intent node
        intent_node = Node(
            id=f"intent_{request_id}",
            type=NodeType.INTENT,
            props={'text': intent_text, 'request_id': request_id},
            tenant_id=tenant_id,
            layer=GraphLayer.SESSION
        )
        self.graph_store.add_node(intent_node)
        
        # Step 2: Gather candidate nodes via graph traversal (with optional vector guidance)
        logger.info(f"Gathering candidates from {len(seed_node_ids)} seed nodes")
        query_embedding = None
        if self.vector_scorer:
            query_embedding = self.vector_scorer.embed_query(intent_text)
            if query_embedding:
                logger.info("Using vector-guided graph traversal")
                candidates = self._gather_candidates_with_vectors(seed_node_ids, query_embedding, tenant_id, trace)
            else:
                logger.info("Falling back to standard graph traversal")
                candidates = self._gather_candidates(seed_node_ids, tenant_id, trace)
        else:
            candidates = self._gather_candidates(seed_node_ids, tenant_id, trace)
        
        trace.candidate_count = len(candidates)
        trace.add_step('gather_candidates', {'count': len(candidates), 'vector_guided': query_embedding is not None})
        
        if not candidates:
            logger.warning("No candidates found")
            trace.final_count = 0
            trace.duration_ms = (time.time() - start_time) * 1000
            return ContextPacket(
                request_id=request_id,
                intent_node_id=intent_node.id,
                nodes=[intent_node],
                edges=[],
                scores={},
                lineage={},
                trace=trace
            )
        
        # Step 3: Score all candidate nodes (with optional vector similarity)
        logger.info(f"Scoring {len(candidates)} candidates")
        scores = self._score_nodes(candidates, query_embedding, trace)
        trace.add_step('score_nodes', {'count': len(scores)})
        
        # Step 4: Prune and select top nodes
        logger.info("Pruning and selecting top nodes")
        selected_nodes = self._prune_and_select(candidates, scores, trace)
        trace.pruned_count = len(candidates) - len(selected_nodes)
        trace.final_count = len(selected_nodes)
        trace.add_step('prune_select', {
            'selected': len(selected_nodes),
            'pruned': trace.pruned_count
        })
        
        # Step 5: Gather edges between selected nodes
        logger.info("Gathering edges between selected nodes")
        selected_edges = self._gather_edges(selected_nodes, tenant_id, trace)
        trace.add_step('gather_edges', {'count': len(selected_edges)})
        
        # Step 6: Build lineage map
        lineage = self._build_lineage(selected_nodes)
        trace.add_step('build_lineage', {'sources': len(lineage)})
        
        # Add intent node to final set
        final_nodes = [intent_node] + selected_nodes
        
        # Create SEEKS_ANSWER_TO edges from intent to seed conversations
        intent_edges = []
        for seed_id in seed_node_ids[:5]:  # Link to top 5 seeds
            seed_node = self.graph_store.get_node(seed_id)
            if seed_node and seed_node in selected_nodes:
                edge = Edge(
                    id=f"edge_intent_{request_id}_{seed_id}",
                    src=intent_node.id,
                    dst=seed_id,
                    type=EdgeType.SEEKS_ANSWER_TO,
                    props={'created_by': 'compiler'},
                    tenant_id=tenant_id,
                    layer=GraphLayer.SESSION
                )
                intent_edges.append(edge)
                self.graph_store.add_edge(edge)
        
        final_edges = intent_edges + selected_edges
        
        trace.duration_ms = (time.time() - start_time) * 1000
        
        # Create context packet
        packet = ContextPacket(
            request_id=request_id,
            intent_node_id=intent_node.id,
            nodes=final_nodes,
            edges=final_edges,
            scores=scores,
            lineage=lineage,
            trace=trace
        )
        
        # Save packet for explainability
        self.graph_store.save_context_packet(packet)
        
        logger.info(f"Compiled context: {len(final_nodes)} nodes, {len(final_edges)} edges in {trace.duration_ms:.2f}ms")
        
        return packet
    
    def _gather_candidates(
        self,
        seed_node_ids: List[str],
        tenant_id: str,
        trace: CompileTrace
    ) -> List[Node]:
        """
        Gather candidate nodes via graph traversal from seeds.
        Expands to connected nodes (documents in threads, users, attachments).
        """
        candidates = []
        visited = set()
        
        for seed_id in seed_node_ids:
            if seed_id in visited:
                continue
            
            # Get seed node (typically a Conversation)
            seed_node = self.graph_store.get_node(seed_id)
            if not seed_node or seed_node.tenant_id != tenant_id:
                continue
            
            candidates.append(seed_node)
            visited.add(seed_id)
            
            # Expand to connected documents (emails in conversation)
            edges = self.graph_store.get_edges_for_node(seed_id)
            for edge in edges:
                # Get connected node
                connected_id = edge.dst if edge.src == seed_id else edge.src
                if connected_id in visited:
                    continue
                
                connected_node = self.graph_store.get_node(connected_id)
                if connected_node and connected_node.tenant_id == tenant_id:
                    candidates.append(connected_node)
                    visited.add(connected_id)
                    
                    # For documents, also get users and attachments
                    if connected_node.type == NodeType.DOCUMENT:
                        doc_edges = self.graph_store.get_edges_for_node(connected_id)
                        for doc_edge in doc_edges:
                            neighbor_id = doc_edge.dst if doc_edge.src == connected_id else doc_edge.src
                            if neighbor_id in visited:
                                continue
                            
                            neighbor = self.graph_store.get_node(neighbor_id)
                            if neighbor and neighbor.tenant_id == tenant_id:
                                if neighbor.type in [NodeType.USER, NodeType.ATTACHMENT]:
                                    candidates.append(neighbor)
                                    visited.add(neighbor_id)
        
        return candidates
    
    def _gather_candidates_with_vectors(
        self,
        seed_node_ids: List[str],
        query_embedding: List[float],
        tenant_id: str,
        trace: CompileTrace
    ) -> List[Node]:
        """
        Gather candidate nodes via vector-guided BFS traversal.
        Uses a priority queue ordered by vector similarity.
        """
        import heapq
        
        # Priority queue: (-similarity, node_id, node)
        pq = []
        visited = set()
        candidates = []
        
        # Initialize with seeds
        for seed_id in seed_node_ids:
            node = self.graph_store.get_node(seed_id)
            if node and node.tenant_id == tenant_id:
                sim = self.vector_scorer.score_node_similarity(node, query_embedding)
                heapq.heappush(pq, (-sim, seed_id, node))
        
        # BFS with priority based on similarity
        while pq and len(candidates) < self.max_nodes * 2:
            neg_sim, node_id, node = heapq.heappop(pq)
            
            if node_id in visited:
                continue
            
            visited.add(node_id)
            candidates.append(node)
            
            # Expand to neighbors, prioritized by similarity
            edges = self.graph_store.get_edges_for_node(node_id)
            for edge in edges:
                neighbor_id = edge.dst if edge.src == node_id else edge.src
                
                if neighbor_id not in visited:
                    neighbor = self.graph_store.get_node(neighbor_id)
                    if neighbor and neighbor.tenant_id == tenant_id:
                        # Score neighbor similarity
                        sim = self.vector_scorer.score_node_similarity(neighbor, query_embedding)
                        heapq.heappush(pq, (-sim, neighbor_id, neighbor))
        
        logger.info(f"Vector-guided BFS gathered {len(candidates)} candidates")
        return candidates
    
    def _score_nodes(self, nodes: List[Node], query_embedding: Optional[List[float]], trace: CompileTrace) -> Dict[str, NodeScore]:
        """
        Score all candidate nodes using multiple signals.
        """
        scores = {}
        now = datetime.utcnow()
        
        for node in nodes:
            score = NodeScore(node_id=node.id)
            
            # Recency score (exponential decay)
            if node.type == NodeType.DOCUMENT:
                received_time_str = node.props.get('received_time')
                if received_time_str:
                    try:
                        received_time = datetime.fromisoformat(received_time_str.replace('Z', '+00:00'))
                        age_days = (now - received_time).total_seconds() / 86400
                        score.recency_score = math.exp(-age_days / self.recency_half_life_days) * 10.0
                    except:
                        score.recency_score = 1.0
                else:
                    score.recency_score = 1.0
            elif node.type == NodeType.CONVERSATION:
                # Use conversation start time
                started_at_str = node.props.get('started_at')
                if started_at_str:
                    try:
                        started_at = datetime.fromisoformat(started_at_str.replace('Z', '+00:00'))
                        age_days = (now - started_at).total_seconds() / 86400
                        score.recency_score = math.exp(-age_days / self.recency_half_life_days) * 10.0
                    except:
                        score.recency_score = 5.0
                else:
                    score.recency_score = 5.0
            else:
                score.recency_score = 1.0
            
            # Authority score
            if node.type == NodeType.USER:
                # Use authority from props or default
                score.authority_score = node.props.get('authority_score', 5.0)
            elif node.type == NodeType.DOCUMENT:
                # Documents inherit authority from sender (simplified)
                score.authority_score = 3.0
            else:
                score.authority_score = 1.0
            
            # Stage score (position in thread)
            if node.type == NodeType.DOCUMENT:
                # Higher score for later messages in thread (more context)
                thread_position = node.props.get('thread_position', 0)
                score.stage_score = min(thread_position * 0.5, 5.0)
            else:
                score.stage_score = 0.0
            
            # Rule score (placeholder for business rules)
            score.rule_score = 0.0
            
            # Vector similarity score (if available)
            if query_embedding and self.vector_scorer:
                try:
                    vector_sim = self.vector_scorer.score_node_similarity(node, query_embedding)
                    # Weight vector similarity heavily (2.0x) as it's most relevant to the query
                    score.total_score += vector_sim * 10.0  # Scale to match other scores
                except Exception as e:
                    logger.error(f"Error computing vector similarity for {node.id}: {str(e)}")
            
            # Compute total
            score.compute_total()
            scores[node.id] = score
        
        return scores
    
    def _prune_and_select(
        self,
        candidates: List[Node],
        scores: Dict[str, NodeScore],
        trace: CompileTrace
    ) -> List[Node]:
        """
        Prune candidates and select top nodes by score.
        """
        # Sort by total score (descending)
        sorted_candidates = sorted(
            candidates,
            key=lambda n: scores[n.id].total_score,
            reverse=True
        )
        
        # Select top nodes within budget
        selected = []
        token_estimate = 0
        
        for node in sorted_candidates:
            if len(selected) >= self.max_nodes:
                break
            
            # Estimate tokens for this node
            node_tokens = self._estimate_node_tokens(node)
            if token_estimate + node_tokens > self.max_tokens:
                break
            
            selected.append(node)
            token_estimate += node_tokens
        
        logger.info(f"Selected {len(selected)}/{len(candidates)} nodes, ~{token_estimate} tokens")
        
        return selected
    
    def _estimate_node_tokens(self, node: Node) -> int:
        """Estimate token count for a node (rough heuristic)."""
        if node.type == NodeType.DOCUMENT:
            body = node.props.get('body_preview', '')
            return len(body.split()) * 1.3  # Rough token estimate
        elif node.type == NodeType.ATTACHMENT:
            text = node.props.get('text_preview', '')
            return len(text.split()) * 1.3
        else:
            return 50  # Small overhead for metadata nodes
    
    def _gather_edges(
        self,
        selected_nodes: List[Node],
        tenant_id: str,
        trace: CompileTrace
    ) -> List[Edge]:
        """
        Gather edges between selected nodes.
        """
        selected_ids = {n.id for n in selected_nodes}
        edges = []
        seen_edge_ids = set()
        
        for node in selected_nodes:
            node_edges = self.graph_store.get_edges_for_node(node.id)
            for edge in node_edges:
                # Only include edges where both endpoints are selected
                if edge.src in selected_ids and edge.dst in selected_ids:
                    if edge.id not in seen_edge_ids and edge.tenant_id == tenant_id:
                        edges.append(edge)
                        seen_edge_ids.add(edge.id)
                        
                        if len(edges) >= self.max_edges:
                            return edges
        
        return edges
    
    def _build_lineage(self, nodes: List[Node]) -> Dict[str, List[str]]:
        """
        Build lineage map showing source attribution.
        """
        lineage = defaultdict(list)
        
        for node in nodes:
            if node.type == NodeType.DOCUMENT:
                email_id = node.props.get('email_id')
                if email_id:
                    lineage['email_ids'].append(email_id)
            elif node.type == NodeType.ATTACHMENT:
                attachment_id = node.props.get('attachment_id')
                filename = node.props.get('filename')
                if attachment_id:
                    lineage['attachment_ids'].append(attachment_id)
                if filename:
                    lineage['attachment_filenames'].append(filename)
        
        return dict(lineage)
